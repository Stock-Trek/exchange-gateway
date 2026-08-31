use crate::{
    clock::Clock,
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    rate_limit::{
        feedback::RateLimitFeedback, rate_limit_config::RateLimitConfig,
        rate_limit_type::RateLimitType, rate_limiter::RateLimiter, rate_limits::RateLimits,
    },
    specs::binance::{
        common::rate_limits, http::connector_with_client_and_rate_limits as connector_with_client,
    },
    transports::{
        http::HttpClientTrait,
        reqwest::{HttpRequest, HttpResponse},
    },
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
    sent: Arc<Mutex<Vec<HttpRequest>>>,
    clock: Clock,
}

#[async_trait]
impl HttpClientTrait for MockHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        endpoint: &str,
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        self.sent.lock().unwrap().push(message);
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

/// Builds an HTTP connector backed by the mock client, handing the caller
/// a handle to the client so sent requests can be inspected. The mock
/// reports the given clock as the server clock on `time` responses, as the
/// production exchange does.
fn mock_http_connector(
    client_handle: std::sync::mpsc::Sender<MockHttpClient>,
    clock: Clock,
) -> EGResult<impl Connector<BinanceHttpUnsignedRequest, BinanceHttpResponse>> {
    let credentials = ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    };
    let to_unsigned_request = Ok;
    let to_external_response = Ok;
    let mock_client = MockHttpClient {
        sent: Arc::new(Mutex::new(Vec::new())),
        clock: clock.clone(),
    };
    let _ = client_handle.send(mock_client.clone());
    let client: Arc<dyn HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse>> =
        Arc::new(mock_client);
    connector_with_client(
        client,
        rate_limits(),
        to_unsigned_request,
        to_external_response,
        Some(credentials),
        clock,
    )
}

#[tokio::test]
async fn http_connector_sync_clock_syncs_the_server_clock() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let clock = Clock::default();
    let connector = mock_http_connector(client_tx, clock).unwrap();
    let client = client_rx.recv().unwrap();

    // Connect establishes the transport only: clock syncing is
    // user-invoked, so the clock is untouched until sync_clock is called.
    connector.connect().await.expect("connect should succeed");

    // Sync clock issues a fresh unsigned time request and adopts the
    // server clock (the mock reports the clock's view of server time).
    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
    let sent = client.sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "sync_clock");
    assert_eq!(
        sent[0].query, None,
        "the sync_clock must be an unsigned time request"
    );
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

/// A scripted HTTP client: records every outgoing request and answers
/// with a fixed outcome, so send-failure budget behaviour can be tested
/// without a network.
#[derive(Clone)]
struct ScriptedHttpClient {
    sent: Arc<Mutex<Vec<HttpRequest>>>,
    outcome: ScriptedOutcome,
}

#[async_trait]
impl HttpClientTrait for ScriptedHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        _endpoint: &str,
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        self.sent.lock().unwrap().push(message);
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
    client_handle: std::sync::mpsc::Sender<ScriptedHttpClient>,
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
    let scripted_client = ScriptedHttpClient {
        sent: Arc::new(Mutex::new(Vec::new())),
        outcome,
    };
    let _ = client_handle.send(scripted_client.clone());
    let client: Arc<dyn HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse>> =
        Arc::new(scripted_client);
    connector_with_client(
        client,
        rate_limits,
        to_unsigned_request,
        to_external_response,
        Some(credentials),
        clock,
    )
}

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

fn single_slot_rate_limits_with_clock(clock: ManualClock) -> RateLimits {
    let clock = Arc::new(move || clock.now());
    RateLimits {
        weight: RateLimiter::with_clock(
            vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }],
            clock.clone(),
        ),
        orders: RateLimiter::with_clock(
            vec![RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            }],
            clock,
        ),
    }
}

fn spot_order_request() -> BinanceHttpUnsignedRequest {
    BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()))
}

#[tokio::test]
async fn http_send_keeps_local_reservation_on_business_rejection() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = scripted_http_connector(
        client_tx,
        ScriptedOutcome::HttpError,
        single_slot_rate_limits(),
        Clock::default(),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

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
    // and never reaches the transport.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_send_refunds_local_reservation_on_rate_limited() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let clock = ManualClock::new();
    let connector = scripted_http_connector(
        client_tx,
        ScriptedOutcome::RateLimited,
        single_slot_rate_limits_with_clock(clock.clone()),
        Clock::default(),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    // A server-side 429 is not counted against the request-weight budget,
    // so the locally-reserved capacity is refunded.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 1);

    // Once the server's Retry-After has elapsed, the refunded budget
    // admits the next request: it reaches the transport again instead of
    // being rejected by the local limiter. The limiter's clock is the
    // manual clock, so the throttle expires without sleeping.
    clock.advance(Duration::from_millis(100));
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 2);
}
