use crate::{
    clock::Clock,
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    rate_limit::{
        feedback::RateLimitFeedback, rate_limit_config::RateLimitConfig,
        rate_limit_type::RateLimitType, rate_limiter::RateLimiter, rate_limits::RateLimits,
    },
    specs::binance::http::{connector, connector_with_client},
    transports::{
        http::HttpClientTrait,
        reqwest::{HttpRequest, HttpResponse},
    },
    urls::TradingMode,
};
use async_trait::async_trait;
use exchange_types::binance::{
    exchange_info::BinanceOrderType,
    http::{BinanceHttpResponse, BinanceHttpResponseResult, BinanceHttpUnsignedRequest},
    spot::{
        BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
        BinanceSpotOrderParams, BinanceTimeInForce,
    },
    time::BinanceTimeResult,
};
use secrecy::SecretString;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

fn spot_order_params() -> BinanceSpotOrderParams {
    BinanceSpotOrderParams {
        apiKey: Some("my-api-key".into()),
        icebergQty: None,
        newClientOrderId: "abc".into(),
        newOrderRespType: BinanceNewOrderResponseType::ACK,
        pegPriceType: None,
        pegOffsetValue: None,
        pegOffsetType: None,
        price: Some("100".parse().unwrap()),
        quantity: Some("1".parse().unwrap()),
        quoteOrderQty: None,
        recvWindow: None,
        selfTradePreventionMode: BinanceSelfTradeProtection::NONE,
        side: BinanceSide::BUY,
        stopPrice: None,
        strategyId: None,
        strategyType: None,
        symbol: "BTCUSDT".into(),
        timeInForce: Some(BinanceTimeInForce::GTC),
        timestamp: 1700000000000,
        trailingDelta: None,
        r#type: BinanceOrderType::LIMIT,
    }
}

#[derive(Clone)]
struct MockHttpClient {
    clock: Clock,
}

#[async_trait]
impl HttpClientTrait for MockHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        endpoint: &str,
        _message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        if endpoint == "time" {
            // sync_clock hits the unsigned `time` endpoint: answer it with
            // the clock's view of server time, as the real exchange would.
            let body = serde_json::to_vec(&BinanceHttpResponseResult::Time(BinanceTimeResult {
                serverTime: self.clock.now_millis(),
            }))
            .expect("serializing a time response should not fail");
            return Ok(HttpResponse {
                status: 200,
                body,
                headers: vec![],
            });
        }
        Ok(HttpResponse {
            status: 200,
            body: br#"[]"#.to_vec(),
            headers: vec![],
        })
    }
}

/// Builds an HTTP connector backed by the mock client. The mock reports the
/// given clock as the server clock on `time` responses, as the production
/// exchange does.
fn mock_http_connector(
    clock: Clock,
) -> EGResult<impl Connector<BinanceHttpUnsignedRequest, BinanceHttpResponse>> {
    let credentials = ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    };
    let to_unsigned_request = Ok;
    let to_external_response = Ok;
    let mock_client = MockHttpClient {
        clock: clock.clone(),
    };
    connector(
        TradingMode::Paper,
        to_unsigned_request,
        to_external_response,
        Some(credentials),
        clock,
        Box::new(move |_url| Ok(mock_client.clone())),
    )
}

#[tokio::test]
async fn http_connector_sync_clock_syncs_the_server_clock() {
    let clock = Clock::default();
    let connector = mock_http_connector(clock).unwrap();

    // Connect establishes the transport only: clock syncing is
    // user-invoked, so the clock is untouched until sync_clock is called.
    connector.connect().await.expect("connect should succeed");

    // Sync clock issues a fresh unsigned time request and adopts the
    // server clock (the mock reports the clock's view of server time).
    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
}

/// The outcome every request answered by a [`ScriptedHttpClient`] takes.
#[derive(Clone)]
enum ScriptedOutcome {
    /// A server-side 429/418 rejection (not counted against the budget).
    RateLimited,
    /// A 4xx/5xx business rejection, e.g. -2010 insufficient balance
    /// (counted against the budget).
    HttpError,
}

/// A scripted HTTP client: answers every outgoing request with a fixed
/// outcome, so send-failure budget behaviour can be tested without a network.
#[derive(Clone)]
struct ScriptedHttpClient {
    outcome: ScriptedOutcome,
}

#[async_trait]
impl HttpClientTrait for ScriptedHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        _endpoint: &str,
        _message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        match self.outcome {
            ScriptedOutcome::RateLimited => Err(EGError::RateLimited(RateLimitFeedback {
                is_throttled: true,
                retry_after: Some(Duration::from_millis(50)),
                usage: vec![],
            })),
            ScriptedOutcome::HttpError => Err(EGError::HttpError {
                status: 400,
                body: br#"{"code":-2010,"msg":"insufficient balance"}"#.to_vec(),
            }),
        }
    }
}

/// Builds an HTTP connector backed by a scripted client answering with
/// `outcome`, using the given rate limits so the budget left after a
/// failed send can be observed.
fn scripted_http_connector(
    outcome: ScriptedOutcome,
    rate_limits: RateLimits,
    clock: Clock,
) -> EGResult<impl Connector<BinanceHttpUnsignedRequest, BinanceHttpResponse>> {
    let credentials = ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    };
    let to_unsigned_request = Ok;
    let to_external_response = Ok;
    connector_with_client(
        Arc::new(ScriptedHttpClient { outcome }),
        rate_limits,
        to_unsigned_request,
        to_external_response,
        Some(credentials),
        clock,
    )
}

/// A controllable clock: `advance` moves `now` forward, so tests can drive
/// time-based throttle expiry without sleeping.
#[derive(Clone)]
struct ManualClock {
    now: Arc<Mutex<Instant>>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
        }
    }
    fn advance(&self, duration: Duration) {
        *self.now.lock().expect("mutex should not be poisoned") += duration;
    }
    fn now(&self) -> Instant {
        *self.now.lock().expect("mutex should not be poisoned")
    }
}

/// Rate limits with a single slot per interval, so one send exhausts the
/// budget and the effect of a refund is immediately observable.
fn single_slot_rate_limits() -> RateLimits {
    RateLimits {
        weight: RateLimiter::new(vec![RateLimitConfig {
            rate_limit_type: RateLimitType::RequestWeight,
            capacity_per_interval: 1,
            interval_nanos: Duration::from_secs(60).as_nanos(),
        }]),
        orders: RateLimiter::new(vec![RateLimitConfig {
            rate_limit_type: RateLimitType::Orders,
            capacity_per_interval: 1,
            interval_nanos: Duration::from_secs(10).as_nanos(),
        }]),
    }
}

/// Single-slot rate limits whose throttle deadline is read from `clock`,
/// so Retry-After expiry can be driven deterministically.
fn single_slot_rate_limits_with_clock(clock: ManualClock) -> RateLimits {
    let now: Arc<dyn Fn() -> Instant + Send + Sync> = Arc::new(move || clock.now());
    RateLimits {
        weight: RateLimiter::with_clock(
            vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }],
            now.clone(),
        ),
        orders: RateLimiter::with_clock(
            vec![RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            }],
            now,
        ),
    }
}

fn spot_order_request() -> BinanceHttpUnsignedRequest {
    BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()))
}

#[tokio::test]
async fn http_send_keeps_local_reservation_on_business_rejection() {
    let connector = scripted_http_connector(
        ScriptedOutcome::HttpError,
        single_slot_rate_limits(),
        Clock::default(),
    )
    .unwrap();

    // The order is rejected with a 4xx business error (-2010 etc.), but
    // Binance counts its weight anyway: the locally-reserved capacity
    // must not be refunded.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(
        result,
        Err(EGError::HttpError { status: 400, .. })
    ));

    // The budget stays exhausted, so the next send is rejected locally
    // with RateLimited before it can reach the transport (a transport hit
    // would be answered with the scripted HttpError, not RateLimited).
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
}

#[tokio::test]
async fn http_send_refunds_local_reservation_on_rate_limited() {
    let clock = ManualClock::new();
    let connector = scripted_http_connector(
        ScriptedOutcome::RateLimited,
        single_slot_rate_limits_with_clock(clock.clone()),
        Clock::default(),
    )
    .unwrap();

    // A server-side 429 is not counted against the request-weight budget:
    // the surfaced error carries the server's retry feedback, and the
    // locally-reserved capacity is refunded.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    match result {
        Err(EGError::RateLimited(feedback)) => {
            assert_eq!(feedback.retry_after, Some(Duration::from_millis(50)));
        }
        other => panic!("expected RateLimited, got: {other:?}"),
    }

    // Once the server's Retry-After has elapsed, the refunded budget
    // admits the next request: it reaches the transport and is answered
    // with the server's 429 again (a local rejection would surface an
    // empty feedback with no retry-after). The limiter's clock is the
    // manual clock, so the throttle expires without sleeping.
    clock.advance(Duration::from_millis(100));
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    match result {
        Err(EGError::RateLimited(feedback)) => {
            assert_eq!(feedback.retry_after, Some(Duration::from_millis(50)));
        }
        other => panic!("expected RateLimited, got: {other:?}"),
    }
}
