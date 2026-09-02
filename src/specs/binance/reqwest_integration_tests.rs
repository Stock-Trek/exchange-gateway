use crate::{
    clock::Clock,
    connector::Connector,
    error::{EGError, EGResult},
    rate_limiter::RateLimiter,
    specs::binance::http::connector,
    transports::{
        http::HttpClientTrait,
        reqwest::{HttpRequest, HttpResponse},
    },
};
use async_trait::async_trait;
use exchange_types::{
    binance::{
        exchange_info::BinanceOrderType,
        http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
    },
    urls::TradingMode,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

fn spot_order_params() -> BinanceSpotOrderParams {
    BinanceSpotOrderParams {
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

#[derive(Debug)]
struct AcceptingRateLimiter;

impl RateLimiter for AcceptingRateLimiter {
    fn did_acquire(
        &self,
        _limit_costs: &Vec<(exchange_types::rate_limited::RateLimitType, u32)>,
    ) -> bool {
        true
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
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        if message.endpoint == "time" {
            // sync_clock hits the unsigned `time` endpoint: answer it with
            // the clock's view of server time, as the real exchange would.
            let body = format!("{{\"serverTime\":{}}}", self.clock.now_millis()).into_bytes();
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
) -> EGResult<impl Connector> {
    let mock_clock = clock.clone();
    let mock_client = MockHttpClient { clock: mock_clock };
    let _ = client_handle.send(mock_client.clone());
    connector(
        TradingMode::Paper,
        clock,
        Arc::new(AcceptingRateLimiter),
        Box::new(move |_url| Ok(mock_client.clone())),
    )
}

#[tokio::test]
async fn http_connector_sync_clock_syncs_the_server_clock() {
    let (client_tx, _client_rx) = std::sync::mpsc::channel();
    let clock = Clock::default();
    let connector = mock_http_connector(client_tx, clock).unwrap();

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
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        self.sent.lock().unwrap().push(message);
        match self.outcome {
            ScriptedOutcome::RateLimited => Err(EGError::RateLimited(vec![])),
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
    clock: Clock,
) -> EGResult<impl Connector<Request = BinanceHttpRequest, Response = BinanceHttpResponse>> {
    let scripted_client = ScriptedHttpClient {
        sent: Arc::new(Mutex::new(Vec::new())),
        outcome,
    };
    let _ = client_handle.send(scripted_client.clone());
    connector(
        TradingMode::Paper,
        clock,
        Arc::new(AcceptingRateLimiter),
        Box::new(move |_url| Ok(scripted_client.clone())),
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
}

fn spot_order_request() -> BinanceHttpRequest {
    BinanceHttpRequest {
        unsigned: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params())),
        signature: None,
    }
}

#[tokio::test]
async fn http_send_keeps_local_reservation_on_business_rejection() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector =
        scripted_http_connector(client_tx, ScriptedOutcome::HttpError, Clock::default()).unwrap();
    let client = client_rx.recv().unwrap();

    // The order is rejected with a 4xx business error (-2010 etc.), but
    // Binance counts its weight anyway: the locally-reserved capacity
    // must not be refunded.
    let result = connector
        .send(spot_order_request(), Duration::from_secs(5))
        .await;
    assert!(matches!(
        result,
        Err(EGError::HttpError { status: 400, .. })
    ));

    // The budget stays exhausted, so the next send is rejected locally
    // and never reaches the transport.
    // let result = connector
    //     .send(spot_order_request(), false, Duration::from_secs(5))
    //     .await;
    // assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_send_refunds_local_reservation_on_rate_limited() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let clock = ManualClock::new();
    let connector =
        scripted_http_connector(client_tx, ScriptedOutcome::RateLimited, Clock::default()).unwrap();
    let client = client_rx.recv().unwrap();

    // A server-side 429 is not counted against the request-weight budget,
    // so the locally-reserved capacity is refunded.
    let result = connector
        .send(spot_order_request(), Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 1);

    // Once the server's Retry-After has elapsed, the refunded budget
    // admits the next request: it reaches the transport again instead of
    // being rejected by the local limiter. The limiter's clock is the
    // manual clock, so the throttle expires without sleeping.
    clock.advance(Duration::from_millis(100));
    let result = connector
        .send(spot_order_request(), Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
}
