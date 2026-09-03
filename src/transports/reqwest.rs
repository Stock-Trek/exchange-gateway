use crate::{
    error::{EGError, EGResult},
    rate_limit::feedback::RateLimitFeedback,
    transports::http::{HttpClientTrait, HttpRequest, HttpResponse},
};
use async_trait::async_trait;
use exchange_types::http::HttpMethod;
use std::time::Duration;

#[derive(Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestHttpClient {
    pub fn new(base_url: &str) -> Self {
        Self::with_client(base_url.trim_end_matches('/'), reqwest::Client::new())
    }
    pub fn with_client(base_url: &str, client: reqwest::Client) -> Self {
        Self {
            client,
            base_url: base_url.into(),
        }
    }
    fn build_url(&self, query: Option<&str>) -> String {
        match query {
            // `query` carries the origin-form request target: the endpoint
            // and any query parameters together (e.g. `"order?symbol=..."`).
            Some(query) if !query.is_empty() => {
                format!("{}/{}", self.base_url, query.trim_start_matches('/'))
            }
            _ => self.base_url.clone(),
        }
    }
    fn method(method: HttpMethod) -> reqwest::Method {
        match method {
            HttpMethod::GET => reqwest::Method::GET,
            HttpMethod::DELETE => reqwest::Method::DELETE,
            HttpMethod::PATCH => reqwest::Method::PATCH,
            HttpMethod::POST => reqwest::Method::POST,
            HttpMethod::PUT => reqwest::Method::PUT,
        }
    }
}

#[async_trait]
impl HttpClientTrait for ReqwestHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        message: Self::TransportReq,
        timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        let url = self.build_url(message.query.as_deref());
        let mut request = self
            .client
            .request(Self::method(message.method), &url)
            .timeout(timeout);
        for (name, value) in &message.headers {
            request = request.header(name, value);
        }
        if let Some(body) = message.body {
            request = request.body(body);
        }
        let response = request.send().await.map_err(|error| {
            if error.is_connect() {
                EGError::NotSent(Box::new(EGError::External(Box::new(error))))
            } else {
                EGError::External(Box::new(error))
            }
        })?;
        let status = response.status();
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
        if status.is_success() {
            Ok(HttpResponse {
                status: status.as_u16(),
                headers,
                body,
            })
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::IM_A_TEAPOT
        {
            Err(EGError::RateLimited(
                rate_limit_feedback_from_status_and_headers(status.as_u16(), &headers),
            ))
        } else {
            Err(EGError::HttpError {
                status: status.as_u16(),
                body,
            })
        }
    }
}

fn rate_limit_feedback_from_status_and_headers(
    status: u16,
    headers: &[(String, String)],
) -> RateLimitFeedback {
    let retry_after = headers
        .iter()
        .find(|(name, _)| name == "retry-after")
        .and_then(|(_, value)| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    RateLimitFeedback {
        retry_after,
        is_throttled: matches!(status, 429 | 418),
        ..Default::default()
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
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
    };

    #[tokio::test]
    async fn send_message_round_trips_through_reqwest() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url =
            spawn_mock_server_with_response(request_log.clone(), 200, "", br#"{"ok":true}"#);
        let client = ReqwestHttpClient::new(&base_url);
        let response = client
            .send_message(
                HttpRequest {
                    method: HttpMethod::POST,
                    query: Some("order?symbol=BTCUSDT".into()),
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
    async fn send_message_maps_4xx_to_http_error() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server_with_response(
            request_log,
            400,
            "",
            br#"{"code":-2014,"msg":"API-key format invalid."}"#,
        );
        let client = ReqwestHttpClient::new(&base_url);
        let error = client
            .send_message(
                HttpRequest {
                    method: HttpMethod::POST,
                    query: Some("order".into()),
                    headers: vec![],
                    body: None,
                },
                Duration::from_secs(5),
            )
            .await
            .expect_err("400 should be returned as an error");
        match error {
            EGError::HttpError { status, body } => {
                assert_eq!(status, 400);
                assert_eq!(body, br#"{"code":-2014,"msg":"API-key format invalid."}"#);
            }
            other => panic!("expected HttpError, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_message_maps_5xx_to_http_error() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server_with_response(
            request_log,
            503,
            "",
            br#"{"code":-1000,"msg":"down"}"#,
        );
        let client = ReqwestHttpClient::new(&base_url);
        let error = client
            .send_message(
                HttpRequest {
                    method: HttpMethod::POST,
                    query: Some("order".into()),
                    headers: vec![],
                    body: None,
                },
                Duration::from_secs(5),
            )
            .await
            .expect_err("503 should be returned as an error");
        match error {
            EGError::HttpError { status, .. } => assert_eq!(status, 503),
            other => panic!("expected HttpError, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_message_maps_429_to_rate_limited_with_retry_after() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server_with_response(
            request_log,
            429,
            "Retry-After: 30",
            br#"{"code":-1003,"msg":"Too many requests"}"#,
        );
        let client = ReqwestHttpClient::new(&base_url);
        let error = client
            .send_message(
                HttpRequest {
                    method: HttpMethod::POST,
                    query: Some("order".into()),
                    headers: vec![],
                    body: None,
                },
                Duration::from_secs(5),
            )
            .await
            .expect_err("429 should be returned as an error");
        let feedback = match error {
            EGError::RateLimited(feedback) => feedback,
            other => panic!("expected RateLimited with feedback, got: {other:?}"),
        };
        assert!(feedback.is_throttled);
        assert_eq!(feedback.retry_after, Some(Duration::from_secs(30)));
        assert!(feedback.usage.is_empty());
    }

    #[tokio::test]
    async fn send_message_maps_connect_failure_to_not_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind to ephemeral port");
        let addr = listener.local_addr().expect("should have local address");
        drop(listener);
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("should build a reqwest client");
        let client = ReqwestHttpClient::with_client(&format!("http://{addr}"), client);
        let error = client
            .send_message(
                HttpRequest {
                    method: HttpMethod::GET,
                    query: None,
                    headers: vec![],
                    body: None,
                },
                Duration::from_secs(5),
            )
            .await
            .expect_err("a refused connection should fail");
        match error {
            EGError::NotSent(inner) => match *inner {
                EGError::External(error) => assert!(
                    error
                        .downcast_ref::<reqwest::Error>()
                        .is_some_and(|error| error.is_connect()),
                    "expected a reqwest connect error, got: {error}"
                ),
                other => panic!("expected External, got: {other:?}"),
            },
            other => panic!("expected NotSent, got: {other:?}"),
        }
    }

    fn spawn_mock_server_with_response(
        request_log: Arc<Mutex<String>>,
        status: u16,
        headers: &str,
        body: &'static [u8],
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind to ephemeral port");
        let addr = listener.local_addr().expect("should have local address");
        let headers = headers.to_string();
        let body = String::from_utf8_lossy(body);
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("should accept a connection");
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).expect("should read the request");
            *request_log.lock().expect("mutex should not be poisoned") =
                String::from_utf8_lossy(&buf[..n]).into_owned();
            let reason = match status {
                200 => "OK",
                400 => "Bad Request",
                429 => "Too Many Requests",
                503 => "Service Unavailable",
                _ => "Error",
            };
            let headers = if headers.is_empty() {
                String::new()
            } else {
                format!("{headers}\r\n")
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("should write the response");
        });
        format!("http://{addr}")
    }
}
