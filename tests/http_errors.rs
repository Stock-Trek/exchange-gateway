use async_trait::async_trait;
use exchange_gateway::{
    clients::client::HttpClient,
    connector::Connector,
    error::{EGError, EGResult},
    server_time_response::ServerTimeResponse,
};
use exchange_types::{
    binance::{
        response::{BinanceResponse, BinanceResponsePayload},
        time::{BinanceTimeRequest, BinanceTimeResponse},
        urls::BinanceUrls,
    },
    encode::ByteEncoder,
    encrypt::Encryptor,
    http::{HttpMethod, HttpRequest, HttpResponse},
    new_types::{Seconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction, RateLimits, RateUsage},
    request::{ETHttpRequest, ETRequest},
    response::{ETHttpResponse, ETResponse},
    signer::Signer,
    urls::TradingMode,
};
use serde::Serialize;
use std::{collections::HashMap, time::Duration};

struct NoRateLimits;

impl RateLimits for NoRateLimits {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        HashMap::new()
    }
}

#[derive(Clone)]
struct StubHttpClient {
    response: HttpResponse,
}

#[async_trait]
impl HttpClient for StubHttpClient {
    async fn send(&self, _request: HttpRequest, _timeout: Duration) -> EGResult<HttpResponse> {
        Ok(self.response.clone())
    }
}

fn connector_with(response: HttpResponse) -> Connector<StubHttpClient> {
    let signer = Signer::new(
        "api-key".into(),
        Encryptor::Ed25519(ed25519_compact::SecretKey::new([7u8; 64])),
        ByteEncoder::Base64,
    );
    Connector::<StubHttpClient>::try_new_http(
        TradingMode::Paper,
        &BinanceUrls,
        NoRateLimits,
        signer,
        Box::new(move |_url: String| {
            Ok(StubHttpClient {
                response: response.clone(),
            })
        }),
    )
    .expect("connector construction must succeed")
}

fn expect_http_error(error: EGError, expected_status: u16, expected_body: &[u8]) {
    match error {
        EGError::HttpError { status, body } => {
            assert_eq!(status, expected_status);
            assert_eq!(body, expected_body);
        }
        other => panic!("expected HttpError, got {other:?}"),
    }
}

#[tokio::test]
async fn json_4xx_error_surfaces_http_error_with_status_and_body() {
    let body = br#"{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}"#;
    let connector = connector_with(HttpResponse {
        status: 400,
        headers: Vec::new(),
        body: body.to_vec(),
    });
    let error = connector
        .send(BinanceTimeRequest::new(), Duration::from_secs(5))
        .await
        .expect_err("a 400 response must not be Ok");
    expect_http_error(error, 400, body);
}

#[tokio::test]
async fn non_json_5xx_error_surfaces_http_error_with_status_and_body() {
    let body = b"<html><body>502 Bad Gateway</body></html>";
    let connector = connector_with(HttpResponse {
        status: 502,
        headers: Vec::new(),
        body: body.to_vec(),
    });
    let error = connector
        .send(BinanceTimeRequest::new(), Duration::from_secs(5))
        .await
        .expect_err("a 502 response must not be Ok");
    expect_http_error(error, 502, body);
}

#[tokio::test]
async fn successful_response_is_parsed_normally() {
    let connector = connector_with(HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: br#"{"serverTime":123}"#.to_vec(),
    });
    let response: BinanceResponse<BinanceTimeResponse> = connector
        .send(BinanceTimeRequest::new(), Duration::from_secs(5))
        .await
        .expect("a 2xx response must parse");
    match response.payload {
        BinanceResponsePayload::Success(result) => assert_eq!(result.serverTime, 123),
        BinanceResponsePayload::Failure(error) => {
            panic!("expected success, got failure: {error:?}")
        }
    }
}

#[tokio::test]
async fn rate_limited_response_with_retry_after_is_rate_limited() {
    let connector = connector_with(HttpResponse {
        status: 429,
        headers: vec![("Retry-After".to_string(), "3".to_string())],
        body: Vec::new(),
    });
    let error = connector
        .send(BinanceTimeRequest::new(), Duration::from_secs(5))
        .await
        .expect_err("a 429 response must not be Ok");
    assert!(
        matches!(error, EGError::RateLimited),
        "expected RateLimited, got {error:?}"
    );
}

#[derive(Serialize)]
struct LocalTimeRequest;

impl ETRequest for LocalTimeRequest {
    fn rate_limit_usage(&self, _restriction: RateLimitRestriction) -> UsageCount {
        UsageCount::ZERO
    }
    fn is_signed(&self) -> bool {
        false
    }
    fn set_api_key(&mut self, _api_key: Option<String>) {}
    fn query_params(&self, _percent_encode: bool) -> String {
        String::new()
    }
}

impl ETHttpRequest for LocalTimeRequest {
    type Response = LocalTimeResponse;

    fn http_method(&self) -> HttpMethod {
        HttpMethod::GET
    }
    fn endpoint(&self) -> &'static str {
        "time"
    }
    fn try_into_http(self, _signer: &Signer) -> exchange_types::error::ETResult<HttpRequest> {
        Ok(HttpRequest {
            method: HttpMethod::GET,
            query: Some("time".into()),
            headers: Vec::new(),
            body: None,
        })
    }
}

struct LocalTimeResponse;

impl ETResponse for LocalTimeResponse {
    fn rate_limit_usage(&self) -> Option<&HashMap<RateLimit, RateUsage>> {
        None
    }
    fn retry_after(&self) -> Option<Seconds> {
        None
    }
}

impl ETHttpResponse for LocalTimeResponse {
    fn try_from_http(_response: HttpResponse) -> exchange_types::error::ETResult<Self> {
        panic!("parsing must not run on a non-2xx response")
    }
}

impl ServerTimeResponse for LocalTimeResponse {
    fn server_time(&self) -> u64 {
        1_000
    }
}

#[tokio::test]
async fn sync_clock_surfaces_non_2xx_status() {
    let body = b"Service Unavailable";
    let connector = connector_with(HttpResponse {
        status: 503,
        headers: Vec::new(),
        body: body.to_vec(),
    });
    let error = connector
        .sync_clock(LocalTimeRequest, Duration::from_secs(5))
        .await
        .expect_err("a 503 response must not be Ok");
    expect_http_error(error, 503, body);
}
