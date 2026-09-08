use async_trait::async_trait;
use exchange_gateway::{
    clients::client::HttpClient,
    connector::Connector,
    error::{EGError, EGResult},
};
use exchange_types::{
    binance::{
        response::BinanceResponse,
        time::{BinanceTimeRequest, BinanceTimeResponse},
        urls::BinanceUrls,
    },
    encode::ByteEncoder,
    encrypt::Encryptor,
    http::{HttpRequest, HttpResponse},
    new_types::{Nanoseconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction, RateLimits},
    signer::Signer,
    urls::TradingMode,
};
use std::{collections::HashMap, time::Duration};

/// A single Weight limiter so Retry-After handling runs against a real
/// limiter (with no limiters configured there is nothing to throttle).
struct WeightLimiter;

impl RateLimits for WeightLimiter {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        HashMap::from([(
            RateLimit {
                restriction: RateLimitRestriction::Weight,
                interval_nanos: Nanoseconds(1_000_000_000),
            },
            UsageCount::ONE,
        )])
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
        WeightLimiter,
        signer,
        Box::new(move |_url: String| {
            Ok(StubHttpClient {
                response: response.clone(),
            })
        }),
    )
    .expect("connector construction must succeed")
}

fn expect_rate_limited(error: EGError) {
    assert!(
        matches!(error, EGError::RateLimited),
        "expected RateLimited, got {error:?}"
    );
}

#[tokio::test]
async fn retry_after_in_seconds_rate_limits() {
    let connector = connector_with(HttpResponse {
        status: 429,
        headers: vec![("Retry-After".to_string(), "3".to_string())],
        body: Vec::new(),
    });
    let response: Result<BinanceResponse<BinanceTimeResponse>, EGError> = connector
        .send(BinanceTimeRequest::new(), Duration::from_secs(5))
        .await;
    expect_rate_limited(response.expect_err("a 429 with Retry-After must not be Ok"));
}

#[tokio::test]
async fn overflowing_retry_after_rate_limits_instead_of_panicking() {
    let connector = connector_with(HttpResponse {
        status: 429,
        headers: vec![("Retry-After".to_string(), u64::MAX.to_string())],
        body: Vec::new(),
    });
    let response: Result<BinanceResponse<BinanceTimeResponse>, EGError> = connector
        .send(BinanceTimeRequest::new(), Duration::from_secs(5))
        .await;
    expect_rate_limited(response.expect_err("a 429 with Retry-After must not be Ok"));
}
