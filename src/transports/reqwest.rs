use crate::{
    error::{EGError, EGResult},
    rate_limit::feedback::{RateLimitFeedback, RateLimitUsage},
    transports::http::HttpClientTrait,
};
use async_trait::async_trait;
use std::time::Duration;

/// A transport-level HTTP request handled by the reqwest-backed client.
///
/// `query` carries the raw query string and is appended to the request URL verbatim.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: reqwest::Method,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

/// A transport-level HTTP response produced by the reqwest-backed client.
///
/// `headers` preserves every response header (lower-cased names) so that
/// exchange rate-limit feedback (`Retry-After`, `X-MBX-*`) survives the
/// transport boundary.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
}

/// A concrete [`HttpClientTrait`] implementation backed by [`reqwest`].
///
/// Requests are sent to `{base_url}/{endpoint}` where `base_url` is fixed at
/// construction time and `endpoint` is supplied per request by
/// [`HttpTransport`](crate::transports::http::HttpTransport).
#[derive(Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestHttpClient {
    /// Creates a client that sends requests to `base_url` using a default
    /// [`reqwest::Client`].
    pub fn new(base_url: &str) -> Self {
        Self::with_client(base_url.trim_end_matches('/'), reqwest::Client::new())
    }
    /// Creates a client that sends requests to `base_url` using a custom
    /// [`reqwest::Client`].
    pub fn with_client(base_url: &str, client: reqwest::Client) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
    fn build_url(&self, endpoint: &str, query: Option<&str>) -> String {
        let mut url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        if let Some(query) = query.filter(|query| !query.is_empty()) {
            url.push('?');
            url.push_str(query);
        }
        url
    }
    fn parse_header(headers: &[(String, String)], name: &str) -> Option<u32> {
        headers
            .iter()
            .find(|(header_name, _)| header_name == name)
            .and_then(|(_, value)| value.trim().parse().ok())
    }
}

#[async_trait]
impl HttpClientTrait for ReqwestHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        endpoint: &str,
        message: Self::TransportReq,
        timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        let url = self.build_url(endpoint, message.query.as_deref());
        let mut request = self.client.request(message.method, &url).timeout(timeout);
        for (name, value) in &message.headers {
            request = request.header(name, value);
        }
        if let Some(body) = message.body {
            request = request.body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| EGError::External(Box::new(e)))?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let body = response
            .bytes()
            .await
            .map_err(|e| EGError::External(Box::new(e)))?
            .to_vec();
        Ok(HttpResponse {
            status,
            body,
            headers,
        })
    }
    fn rate_limit_feedback(&self, response: &Self::TransportRes) -> RateLimitFeedback {
        // Binance's REST API signals throttling via 429 (too many requests) /
        // 418 (IP auto-banned) with an optional Retry-After header, and reports
        // actual usage on every response via X-MBX-* headers. Feeding these
        // into the local limiter keeps it aligned with the server.
        let retry_after = response
            .headers
            .iter()
            .find(|(name, _)| name == "retry-after")
            .and_then(|(_, value)| value.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let mut feedback = RateLimitFeedback {
            retry_after,
            throttled: matches!(response.status, 429 | 418),
            ..Default::default()
        };
        if let Some(used) = Self::parse_header(&response.headers, "x-mbx-used-weight-1m") {
            feedback.usage.push(RateLimitUsage {
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used,
                limit: None,
            });
        }
        if let Some(used) = Self::parse_header(&response.headers, "x-mbx-order-count-10s") {
            feedback.usage.push(RateLimitUsage {
                interval_nanos: Duration::from_secs(10).as_nanos(),
                used,
                limit: None,
            });
        }
        if let Some(used) = Self::parse_header(&response.headers, "x-mbx-order-count-1d") {
            feedback.usage.push(RateLimitUsage {
                interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                used,
                limit: None,
            });
        }
        feedback
    }
}

impl std::fmt::Debug for ReqwestHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestHttpClient")
            .field("client", &"<reqwest::Client>")
            .field("base_url", &self.base_url)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transports::http::HttpClientTrait;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
    };

    #[tokio::test]
    async fn send_message_round_trips_through_reqwest() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server(request_log.clone(), "200 OK", "");
        let client = ReqwestHttpClient::new(&base_url);
        let response = client
            .send_message(
                "order",
                HttpRequest {
                    method: reqwest::Method::POST,
                    query: Some("symbol=BTCUSDT".into()),
                    headers: vec![("X-Test".into(), "abc".into())],
                    body: Some(b"hello".to_vec()),
                },
                Duration::from_secs(5),
            )
            .await
            .expect("request should succeed");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, br#"{"ok":true}"#);
        let request = request_log.lock().expect("mutex should not be poisoned");
        assert!(request.starts_with("POST /order?symbol=BTCUSDT HTTP/1.1"));
        assert!(request.contains("x-test: abc"));
        assert!(request.ends_with("hello"));
    }

    #[tokio::test]
    async fn rate_limit_feedback_parses_usage_and_retry_after() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server(
            request_log.clone(),
            "429 Too Many Requests",
            "Retry-After: 30\r\nX-MBX-USED-WEIGHT-1M: 6000\r\nX-MBX-ORDER-COUNT-10S: 3",
        );
        let client = ReqwestHttpClient::new(&base_url);
        let response = client
            .send_message(
                "order",
                HttpRequest {
                    method: reqwest::Method::POST,
                    query: None,
                    headers: vec![],
                    body: None,
                },
                Duration::from_secs(5),
            )
            .await
            .expect("request should succeed");
        let feedback = client.rate_limit_feedback(&response);
        assert!(feedback.throttled);
        assert_eq!(feedback.retry_after, Some(Duration::from_secs(30)));
        assert_eq!(feedback.usage.len(), 2);
        assert_eq!(
            feedback.usage[0].interval_nanos,
            Duration::from_secs(60).as_nanos()
        );
        assert_eq!(feedback.usage[0].used, 6000);
        assert_eq!(feedback.usage[0].limit, None);
        assert_eq!(
            feedback.usage[1].interval_nanos,
            Duration::from_secs(10).as_nanos()
        );
        assert_eq!(feedback.usage[1].used, 3);
    }

    fn spawn_mock_server(
        request_log: Arc<Mutex<String>>,
        status_line: &str,
        headers: &str,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind to ephemeral port");
        let addr = listener.local_addr().expect("should have local address");
        let status_line = status_line.to_string();
        let headers = headers.to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("should accept a connection");
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).expect("should read the request");
            *request_log.lock().expect("mutex should not be poisoned") =
                String::from_utf8_lossy(&buf[..n]).into_owned();
            let body = br#"{"ok":true}"#;
            let header_block = if headers.is_empty() {
                String::new()
            } else {
                format!("{headers}\r\n")
            };
            let response = format!(
                "HTTP/1.1 {status_line}\r\n{header_block}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                String::from_utf8_lossy(body)
            );
            stream
                .write_all(response.as_bytes())
                .expect("should write the response");
        });
        format!("http://{addr}")
    }
}
