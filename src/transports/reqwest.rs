use crate::{
    error::{EGError, EGResult},
    transports::http::HttpClientTrait,
};
use async_trait::async_trait;
use futures_timer::Delay;
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
/// `retry_after` is populated from the `Retry-After` response header (in
/// seconds) whenever it is present and parseable. It lets callers honour the
/// server's back-off instruction for rate-limited or overloaded endpoints.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub retry_after: Option<Duration>,
}

/// Configuration for retrying failed HTTP requests with exponential backoff.
///
/// A request is retried when the connection fails, when the request times
/// out, or when the server answers with a transient status code (408, 429 or
/// any 5xx). Client errors (4xx) are never retried, and a successful response
/// is returned as-is.
///
/// Backoff doubles after each failed attempt (`initial_backoff * 2^(attempt-1)`)
/// and is capped at `max_backoff`. If a retried response carries a
/// `Retry-After` header, the longer of that and the computed backoff is used,
/// so the server's explicit instruction is always respected.
///
/// # Examples
///
/// ```
/// use exchange_gateway::transports::reqwest::RetryConfig;
/// use std::time::Duration;
///
/// let config = RetryConfig::new()
///     .with_max_attempts(5)
///     .with_initial_backoff(Duration::from_millis(200))
///     .with_max_backoff(Duration::from_secs(10));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryConfig {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl RetryConfig {
    /// Creates a configuration with production-sensible defaults:
    /// up to 3 attempts, 100ms initial backoff and 5s maximum backoff.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of attempts, including the initial request.
    /// The value is clamped to at least 1.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    /// Sets the backoff applied after the first failed attempt.
    pub fn with_initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// Sets the upper bound for the exponential backoff.
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Computes the backoff to apply before attempt `attempt` (1-based).
    fn backoff(&self, attempt: u32) -> Duration {
        debug_assert!(attempt >= 1);
        let exponent = attempt.saturating_sub(1).min(63);
        let factor = 1u64 << exponent;
        let nanos = self
            .initial_backoff
            .as_nanos()
            .saturating_mul(factor as u128)
            .min(self.max_backoff.as_nanos());
        Duration::from_nanos(nanos as u64)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Returns true when `status` indicates a transient failure worth retrying.
fn is_retryable_status(status: u16) -> bool {
    status == 408 || status == 429 || (500..=599).contains(&status)
}

/// Returns true when `error` indicates the request may not have been
/// processed (connection failure or timeout), so a retry is safe.
fn is_retryable_error(error: &reqwest::Error) -> bool {
    error.is_connect() || error.is_timeout()
}

/// A concrete [`HttpClientTrait`] implementation backed by [`reqwest`].
///
/// Requests are sent to `{base_url}/{endpoint}` where `base_url` is fixed at
/// construction time and `endpoint` is supplied per request by
/// [`HttpTransport`](crate::transports::http::HttpTransport).
///
/// Transient failures (connection errors, timeouts, 408/429/5xx responses)
/// are retried with exponential backoff as configured by [`RetryConfig`];
/// see [`RetryConfig::new`] for the defaults.
#[derive(Clone)]
pub struct ReqwestHttpClient {
    client: reqwest::Client,
    base_url: String,
    retry: RetryConfig,
}

impl ReqwestHttpClient {
    /// Creates a client that sends requests to `base_url` using a default
    /// [`reqwest::Client`] and the default [`RetryConfig`].
    pub fn new(base_url: &str) -> Self {
        Self::with_client(base_url, reqwest::Client::new())
    }

    /// Creates a client that sends requests to `base_url` using a custom
    /// [`reqwest::Client`] and the default [`RetryConfig`].
    pub fn with_client(base_url: &str, client: reqwest::Client) -> Self {
        Self::with_client_and_retry(base_url, client, RetryConfig::new())
    }

    /// Creates a client that sends requests to `base_url` using a custom
    /// [`reqwest::Client`] and the supplied [`RetryConfig`].
    pub fn with_client_and_retry(
        base_url: &str,
        client: reqwest::Client,
        retry: RetryConfig,
    ) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').into(),
            retry,
        }
    }

    /// Replaces the retry/backoff configuration of this client.
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    fn build_url(&self, endpoint: &str, query: Option<&str>) -> String {
        let mut url = format!("{}/{}", self.base_url, endpoint.trim_start_matches('/'));
        if let Some(query) = query.filter(|query| !query.is_empty()) {
            url.push('?');
            url.push_str(query);
        }
        url
    }

    /// Backoff to apply after a failed attempt, preferring the server's
    /// `Retry-After` instruction when present.
    fn retry_delay(&self, response: &HttpResponse, attempt: u32) -> Duration {
        let backoff = self.retry.backoff(attempt);
        response
            .retry_after
            .map(|retry_after| retry_after.max(backoff))
            .unwrap_or(backoff)
    }

    async fn send_once(
        &self,
        url: &str,
        message: &HttpRequest,
        timeout: Duration,
    ) -> Result<HttpResponse, reqwest::Error> {
        let mut request = self
            .client
            .request(message.method.clone(), url)
            .timeout(timeout);
        for (name, value) in &message.headers {
            request = request.header(name, value);
        }
        if let Some(body) = &message.body {
            request = request.body(body.clone());
        }
        let response = request.send().await?;
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);
        let status = response.status().as_u16();
        let body = response.bytes().await?.to_vec();
        Ok(HttpResponse {
            status,
            body,
            retry_after,
        })
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
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match self.send_once(&url, &message, timeout).await {
                Ok(response) => {
                    if attempt < self.retry.max_attempts && is_retryable_status(response.status) {
                        Delay::new(self.retry_delay(&response, attempt)).await;
                        continue;
                    }
                    return Ok(response);
                }
                Err(error) => {
                    if attempt < self.retry.max_attempts && is_retryable_error(&error) {
                        Delay::new(self.retry.backoff(attempt)).await;
                        continue;
                    }
                    return Err(EGError::External(Box::new(error)));
                }
            }
        }
    }
}

impl std::fmt::Debug for ReqwestHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReqwestHttpClient")
            .field("client", &"<reqwest::Client>")
            .field("base_url", &self.base_url)
            .field("retry", &self.retry)
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
        assert_eq!(response.retry_after, None);
        let request = request_log.lock().expect("mutex should not be poisoned");
        assert!(request.starts_with("POST /order?symbol=BTCUSDT HTTP/1.1"));
        assert!(request.contains("x-test: abc"));
        assert!(request.ends_with("hello"));
    }

    fn spawn_mock_server(request_log: Arc<Mutex<String>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind to ephemeral port");
        let addr = listener.local_addr().expect("should have local address");
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("should accept a connection");
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).expect("should read the request");
            *request_log.lock().expect("mutex should not be poisoned") =
                String::from_utf8_lossy(&buf[..n]).into_owned();
            let body = br#"{"ok":true}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                String::from_utf8_lossy(body)
            );
            stream
                .write_all(response.as_bytes())
                .expect("should write the response");
        });
        format!("http://{addr}")
    }

    /// A scripted response served by [`spawn_scripted_server`].
    #[derive(Debug, Clone)]
    struct ScriptedResponse {
        status: u16,
        body: &'static [u8],
        retry_after: Option<u64>,
    }

    /// Spawns a server that answers each connection with the next scripted
    /// response, recording every received request.
    fn spawn_scripted_server(
        responses: Vec<ScriptedResponse>,
    ) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("should bind to ephemeral port");
        let addr = listener.local_addr().expect("should have local address");
        let request_log = Arc::new(Mutex::new(Vec::new()));
        let log = request_log.clone();
        std::thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("should accept a connection");
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).expect("should read the request");
                log.lock().expect("mutex should not be poisoned").push(
                    String::from_utf8_lossy(&buf[..n]).into_owned(),
                );
                let mut head = format!("HTTP/1.1 {} X\r\n", response.status);
                if let Some(seconds) = response.retry_after {
                    head.push_str(&format!("Retry-After: {seconds}\r\n"));
                }
                head.push_str(&format!(
                    "Content-Length: {}\r\nConnection: close\r\n\r\n",
                    response.body.len()
                ));
                stream
                    .write_all(head.as_bytes())
                    .expect("should write the head");
                stream
                    .write_all(response.body)
                    .expect("should write the body");
            }
        });
        (format!("http://{addr}"), request_log)
    }

    fn test_client(base_url: &str) -> ReqwestHttpClient {
        ReqwestHttpClient::with_client_and_retry(
            base_url,
            reqwest::Client::new(),
            RetryConfig::new()
                .with_max_attempts(3)
                .with_initial_backoff(Duration::from_millis(1))
                .with_max_backoff(Duration::from_millis(2)),
        )
    }

    fn get_request() -> HttpRequest {
        HttpRequest {
            method: reqwest::Method::GET,
            query: None,
            headers: vec![],
            body: None,
        }
    }

    #[tokio::test]
    async fn retries_transient_server_errors_until_success() {
        let (base_url, request_log) = spawn_scripted_server(vec![
            ScriptedResponse {
                status: 503,
                body: b"{\"err\":\"busy\"}",
                retry_after: None,
            },
            ScriptedResponse {
                status: 503,
                body: b"{\"err\":\"busy\"}",
                retry_after: None,
            },
            ScriptedResponse {
                status: 200,
                body: br#"{"ok":true}"#,
                retry_after: None,
            },
        ]);
        let client = test_client(&base_url);
        let response = client
            .send_message("order", get_request(), Duration::from_secs(5))
            .await
            .expect("request should eventually succeed");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, br#"{"ok":true}"#);
        assert_eq!(
            request_log.lock().expect("mutex should not be poisoned").len(),
            3,
            "503s should be retried"
        );
    }

    #[tokio::test]
    async fn exhausts_retries_and_returns_last_response() {
        let (base_url, request_log) = spawn_scripted_server(vec![
            ScriptedResponse {
                status: 502,
                body: b"",
                retry_after: None,
            },
            ScriptedResponse {
                status: 502,
                body: b"",
                retry_after: None,
            },
            ScriptedResponse {
                status: 502,
                body: b"",
                retry_after: None,
            },
        ]);
        let client = test_client(&base_url);
        let response = client
            .send_message("order", get_request(), Duration::from_secs(5))
            .await
            .expect("the last response should be returned");
        assert_eq!(response.status, 502);
        assert_eq!(
            request_log.lock().expect("mutex should not be poisoned").len(),
            3,
            "retries should stop after max_attempts"
        );
    }

    #[tokio::test]
    async fn does_not_retry_client_errors() {
        let (base_url, request_log) = spawn_scripted_server(vec![ScriptedResponse {
            status: 400,
            body: br#"{"err":"bad"}"#,
            retry_after: None,
        }]);
        let client = test_client(&base_url);
        let response = client
            .send_message("order", get_request(), Duration::from_secs(5))
            .await
            .expect("request should complete");
        assert_eq!(response.status, 400);
        assert_eq!(
            request_log.lock().expect("mutex should not be poisoned").len(),
            1,
            "client errors should not be retried"
        );
    }

    #[tokio::test]
    async fn respects_retry_after_header() {
        let (base_url, request_log) = spawn_scripted_server(vec![
            ScriptedResponse {
                status: 429,
                body: b"",
                retry_after: Some(0),
            },
            ScriptedResponse {
                status: 200,
                body: br#"{"ok":true}"#,
                retry_after: None,
            },
        ]);
        let client = test_client(&base_url);
        let response = client
            .send_message("order", get_request(), Duration::from_secs(5))
            .await
            .expect("request should eventually succeed");
        assert_eq!(response.status, 200);
        assert_eq!(
            request_log.lock().expect("mutex should not be poisoned").len(),
            2,
            "rate-limited responses should be retried"
        );
    }

    #[tokio::test]
    async fn captures_retry_after_header() {
        let (base_url, _request_log) = spawn_scripted_server(vec![ScriptedResponse {
            status: 429,
            body: b"",
            retry_after: Some(30),
        }]);
        let client = ReqwestHttpClient::with_client_and_retry(
            &base_url,
            reqwest::Client::new(),
            RetryConfig::new().with_max_attempts(1),
        );
        let response = client
            .send_message("order", get_request(), Duration::from_secs(5))
            .await
            .expect("request should complete");
        assert_eq!(response.status, 429);
        assert_eq!(response.retry_after, Some(Duration::from_secs(30)));
    }

    #[test]
    fn backoff_grows_exponentially_and_caps_at_max() {
        let config = RetryConfig::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_max_backoff(Duration::from_millis(400));
        assert_eq!(config.backoff(1), Duration::from_millis(100));
        assert_eq!(config.backoff(2), Duration::from_millis(200));
        assert_eq!(config.backoff(3), Duration::from_millis(400));
        assert_eq!(config.backoff(4), Duration::from_millis(400));
    }

    #[test]
    fn max_attempts_is_at_least_one() {
        assert_eq!(RetryConfig::new().with_max_attempts(0).max_attempts, 1);
    }
}
