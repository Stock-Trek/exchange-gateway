use crate::{
    error::{EGError, EGResult},
    transports::http::HttpClientTrait,
};
use async_trait::async_trait;
use std::time::Duration;

/// A transport-level HTTP request handled by the reqwest-backed client.
///
/// `query` carries the raw query string and is appended to the request URL verbatim.
#[derive(Debug, Clone)]
pub(crate) struct HttpRequest {
    pub(crate) method: reqwest::Method,
    pub(crate) query: Option<String>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Option<Vec<u8>>,
}

/// A transport-level HTTP response produced by the reqwest-backed client.
///
/// Only successful (2xx) responses are ever returned; error statuses (4xx,
/// 429 and 5xx) are surfaced as [`EGError`] by
/// [`ReqwestHttpClient::send_message`].
#[derive(Debug, Clone)]
pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

/// A concrete [`HttpClientTrait`] implementation backed by [`reqwest`].
///
/// Requests are sent to `{base_url}/{endpoint}` where `base_url` is fixed at
/// construction time and `endpoint` is supplied per request by
/// [`HttpTransport`](crate::transports::http::HttpTransport).
///
/// Non-success HTTP statuses are returned as errors: 429 maps to
/// [`EGError::RateLimited`] and every other non-2xx status maps to
/// [`EGError::HttpError`] (which carries the response body so the exchange's
/// error message can be inspected).
#[derive(Clone)]
pub(crate) struct ReqwestHttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl ReqwestHttpClient {
    /// Creates a client that sends requests to `base_url` using a default
    /// [`reqwest::Client`].
    pub(crate) fn new(base_url: &str) -> Self {
        Self::with_client(base_url.trim_end_matches('/'), reqwest::Client::new())
    }
    /// Creates a client that sends requests to `base_url` using a custom
    /// [`reqwest::Client`].
    pub(crate) fn with_client(base_url: &str, client: reqwest::Client) -> Self {
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
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| EGError::External(Box::new(e)))?
            .to_vec();
        if status.is_success() {
            Ok(HttpResponse {
                status: status.as_u16(),
                body,
            })
        } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Err(EGError::RateLimited)
        } else {
            Err(EGError::HttpError {
                status: status.as_u16(),
                body,
            })
        }
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
        let base_url = spawn_mock_server(request_log.clone());
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
    async fn send_message_maps_429_to_rate_limited() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server_with_response(
            request_log,
            429,
            br#"{"code":-1003,"msg":"Too many requests"}"#,
        );
        let client = ReqwestHttpClient::new(&base_url);
        let error = client
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
            .expect_err("429 should be returned as an error");
        assert!(
            matches!(error, EGError::RateLimited),
            "expected RateLimited, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn send_message_maps_4xx_to_http_error() {
        let request_log = Arc::new(Mutex::new(String::new()));
        let base_url = spawn_mock_server_with_response(
            request_log,
            400,
            br#"{"code":-2014,"msg":"API-key format invalid."}"#,
        );
        let client = ReqwestHttpClient::new(&base_url);
        let error = client
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
        let base_url =
            spawn_mock_server_with_response(request_log, 503, br#"{"code":-1000,"msg":"down"}"#);
        let client = ReqwestHttpClient::new(&base_url);
        let error = client
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
            .expect_err("503 should be returned as an error");
        match error {
            EGError::HttpError { status, .. } => assert_eq!(status, 503),
            other => panic!("expected HttpError, got: {other:?}"),
        }
    }

    fn spawn_mock_server(request_log: Arc<Mutex<String>>) -> String {
        spawn_mock_server_with_response(request_log, 200, br#"{"ok":true}"#)
    }

    fn spawn_mock_server_with_response(
        request_log: Arc<Mutex<String>>,
        status: u16,
        body: &'static [u8],
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind to ephemeral port");
        let addr = listener.local_addr().expect("should have local address");
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
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
