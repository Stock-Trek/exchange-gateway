use crate::{
    clock::Clock,
    connector::Connector,
    error::{EGError, EGResult},
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
    rate_limiter::RateLimiter,
    specs::binance::websocket::connector,
    transports::websocket::WebsocketClientTrait,
};
use async_trait::async_trait;
use exchange_types::{
    binance::{
        error::BinanceError,
        exchange_info::BinanceOrderType,
        logon::BinanceLogonParams,
        signature::BinanceSignature,
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
        time::BinanceTimeResult,
        websocket::{
            BinanceWebsocketMethodName, BinanceWebsocketRequest, BinanceWebsocketResponse,
            BinanceWebsocketResponseResult, BinanceWebsocketSignedParams,
            BinanceWebsocketUnsignedParams,
        },
    },
    urls::TradingMode,
};
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn logon_response(
    id: String,
    status: i32,
    error: Option<BinanceError>,
) -> BinanceWebsocketResponse {
    BinanceWebsocketResponse {
        error,
        id,
        rateLimits: vec![],
        result: None,
        status,
    }
}

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
struct MockWebsocketClient {
    listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
    connected: Arc<AtomicBool>,
    sent: Arc<Mutex<Vec<BinanceWebsocketRequest>>>,
    logon_gate: Option<LogonGate>,
    logon_error: Option<BinanceError>,
    clock: Clock,
}

#[derive(Clone)]
struct LogonGate {
    block: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
    fail: Arc<AtomicBool>,
}

#[async_trait]
impl WebsocketClientTrait for MockWebsocketClient {
    type TransportReq = BinanceWebsocketRequest;
    type TransportRes = BinanceWebsocketResponse;

    async fn connect(&self) -> EGResult<()> {
        self.connected.store(true, Ordering::SeqCst);
        self.listener.on_connected().await
    }
    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }
    async fn send_message(&self, message: Self::TransportReq, _timeout: Duration) -> EGResult<()> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(EGError::External(Box::new(
                iris::ConnectionError::ConnectionClosed,
            )));
        }
        self.sent
            .lock()
            .expect("mutex should not be poisoned")
            .push(message.clone());
        match message.params.unsigned.method_name() {
            BinanceWebsocketMethodName::Logon => {
                if let Some(gate) = &self.logon_gate {
                    if gate.fail.load(Ordering::SeqCst) {
                        return Err(EGError::TimedOut);
                    }
                    if gate.block.load(Ordering::SeqCst) {
                        gate.release.notified().await;
                    }
                }
                let response = match &self.logon_error {
                    Some(error) => logon_response(message.id, 401, Some(error.clone())),
                    None => logon_response(message.id, 200, None),
                };
                self.listener.on_message(response).await?;
            }
            BinanceWebsocketMethodName::Time => {
                let response = BinanceWebsocketResponse {
                    error: None,
                    id: message.id,
                    rateLimits: vec![],
                    result: Some(BinanceWebsocketResponseResult::Time(BinanceTimeResult {
                        serverTime: self.clock.now_millis(),
                    })),
                    status: 200,
                };
                self.listener.on_message(response).await?;
            }
            // Every user request (order, exchangeInfo, ...) is answered with
            // a matching-id success reply, as the real exchange does, so the
            // send-and-wait call resolves.
            _ => {
                let response = BinanceWebsocketResponse {
                    error: None,
                    id: message.id,
                    rateLimits: vec![],
                    result: None,
                    status: 200,
                };
                self.listener.on_message(response).await?;
            }
        }
        Ok(())
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.connected.store(false, Ordering::SeqCst);
        self.listener.on_disconnected().await
    }
}

struct IgnoreListener;

#[async_trait]
impl ListenerTrait for IgnoreListener {
    type TMessage = BinanceWebsocketResponse;

    async fn on_message(&self, _message: BinanceWebsocketResponse) -> EGResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct RecordingListener {
    received: Arc<Mutex<Vec<BinanceWebsocketResponse>>>,
}

#[async_trait]
impl ListenerTrait for RecordingListener {
    type TMessage = BinanceWebsocketResponse;

    async fn on_message(&self, message: BinanceWebsocketResponse) -> EGResult<()> {
        self.received
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .push(message);
        Ok(())
    }
}

fn mock_session_connector(
    client_handle: std::sync::mpsc::Sender<MockWebsocketClient>,
    logon_gate: Option<LogonGate>,
    logon_error: Option<BinanceError>,
    listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
    clock: Clock,
) -> EGResult<impl Connector<Request = BinanceWebsocketRequest, Response = BinanceWebsocketResponse>>
{
    let clock_clone = clock.clone();
    let logon_gate_clone = logon_gate.clone();
    let logon_error_clone = logon_error.clone();
    let client_creator = Box::new(
        move |(_url, websocket_listener): (
            &str,
            Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
        )| {
            let mock_client = MockWebsocketClient {
                listener: websocket_listener,
                connected: Arc::new(AtomicBool::new(false)),
                sent: Arc::new(Mutex::new(Vec::new())),
                logon_gate: logon_gate_clone,
                logon_error: logon_error_clone,
                clock: clock_clone,
            };
            let _ = client_handle.send(mock_client.clone());
            Ok(mock_client)
        },
    );
    connector(
        TradingMode::Paper,
        clock,
        Arc::new(AcceptingRateLimiter),
        listener,
        client_creator,
    )
}

fn logon_request() -> BinanceWebsocketRequest {
    BinanceWebsocketRequest {
        id: "logon-1".into(),
        params: BinanceWebsocketSignedParams {
            unsigned: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams { timestamp: 0 }),
            signature: Some(BinanceSignature {
                apiKey: "api-key".into(),
                signature: "signature".into(),
            }),
        },
    }
}

fn order_request() -> BinanceWebsocketRequest {
    BinanceWebsocketRequest {
        id: "order-1".into(),
        params: BinanceWebsocketSignedParams {
            unsigned: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(
                spot_order_params(),
            )),
            signature: Some(BinanceSignature {
                apiKey: "api-key".into(),
                signature: "signature".into(),
            }),
        },
    }
}

fn logon_count(sent: &[BinanceWebsocketRequest]) -> usize {
    sent.iter()
        .filter(|message| {
            matches!(
                message.params.unsigned.method_name(),
                BinanceWebsocketMethodName::Logon
            )
        })
        .count()
}

/// Runs `body` with the runtime clock paused so it can drive timers with
/// `advance` instead of sleeping. The clock is resumed before returning.
async fn with_paused_clock<T>(body: impl Future<Output = T>) -> T {
    tokio::time::pause();
    let result = body.await;
    tokio::time::resume();
    result
}

/// Advances the paused clock by `step` and yields so connector tasks can
/// make progress.
async fn tick(step: Duration) {
    tokio::time::advance(step).await;
    tokio::task::yield_now().await;
}

/// Polls `condition` until it holds, driving the runtime clock forward with
/// `pause`/`advance` instead of sleeping.
async fn wait_until(mut condition: impl FnMut() -> bool) -> Option<()> {
    with_paused_clock(async {
        for _ in 0..500 {
            if condition() {
                return Some(());
            }
            tick(Duration::from_millis(10)).await;
        }
        None
    })
    .await
}

#[tokio::test]
async fn post_logon_requests_omit_signature() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector =
        mock_session_connector(client_tx, None, None, IgnoreListener, Clock::default()).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    connector
        .send(logon_request(), Duration::from_secs(5))
        .await
        .expect("send should succeed");
    connector
        .send(order_request(), Duration::from_secs(5))
        .await
        .expect("send should succeed");

    // After session.logon the connection is authenticated, so the order must
    // omit the signature.
    let sent = client.sent.lock().unwrap();
    let order = sent
        .iter()
        .find(|message| {
            matches!(
                message.params.unsigned.method_name(),
                BinanceWebsocketMethodName::PlaceOrder
            )
        })
        .expect("the order should have been sent");
    assert!(order.params.signature.is_none());
    assert!(matches!(
        order.params.unsigned,
        BinanceWebsocketUnsignedParams::SpotOrderRequest(..)
    ));
}

#[tokio::test]
async fn sends_during_a_drop_fail_fast_until_reconnect() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = Arc::new(
        mock_session_connector(client_tx, None, None, IgnoreListener, Clock::default()).unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // The connection drops. The transport reports the disconnect (as iris
    // does when the socket closes, before it reconnects), bumping the
    // connection epoch so the session is stale while the connection is
    // down and re-authentication cannot be bypassed.
    client
        .disconnect()
        .await
        .expect("disconnect should succeed");

    // A signed send while the connection is down must fail fast with
    // `NotAuthenticated`: authentication is user-invoked rather than
    // automatic, so no re-authentication logon is attempted while the
    // connection is down and the order is never queued under a dead
    // session.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(order_request(), Duration::from_secs(5)),
    )
    .await
    .expect("send while the connection is down should fail fast, not hang")
    .expect_err("send must fail while the connection is down");
    assert!(matches!(error, EGError::NotAuthenticated));
    // Neither the logon nor the order was ever recorded: the fail-fast
    // happens before any message reaches the transport.
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);
    assert!(!client.sent.lock().unwrap().iter().any(|message| matches!(
        message.params.unsigned.method_name(),
        BinanceWebsocketMethodName::PlaceOrder
    )));

    // Once the connection comes back, the user re-authenticates and the
    // signed send goes out normally.
    client.connect().await.expect("reconnect should succeed");
    connector
        .send(order_request(), Duration::from_secs(5))
        .await
        .expect("send should succeed once the connection returns");
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 2);
    assert!(client.sent.lock().unwrap().iter().any(|message| matches!(
        message.params.unsigned.method_name(),
        BinanceWebsocketMethodName::PlaceOrder
    )));
}

#[tokio::test]
async fn logon_sent_while_reconnecting_fails_fast_and_leaves_nothing_pending() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let received = Arc::new(Mutex::new(Vec::new()));
    let listener = RecordingListener {
        received: received.clone(),
    };
    let connector = Arc::new(
        mock_session_connector(client_tx, None, None, listener, Clock::default()).unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // The connection drops
    client
        .disconnect()
        .await
        .expect("disconnect should succeed");
    assert!(!connector.is_connected().unwrap());

    // A signed send while the connection is down must fail fast with
    // `NotAuthenticated`: authentication is user-invoked, so no logon is
    // attempted for the dead connection and nothing is left queued to
    // resolve (or confuse) a later authentication.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(order_request(), Duration::from_secs(5)),
    )
    .await
    .expect("send while reconnecting should fail fast, not hang until its timeout")
    .expect_err("the stale session must fail the send");
    assert!(matches!(error, EGError::NotAuthenticated));
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // The connection comes back and the session is stale again, so the
    // user's next authentication sends a fresh logon with an id distinct
    // from the initial one, and the signed send goes out normally.
    client.connect().await.expect("reconnect should succeed");
    let retried_send = {
        let connector = connector.clone();
        tokio::spawn(async move {
            connector
                .send(order_request(), Duration::from_secs(5))
                .await
                .expect("the retried send should succeed against its own logon")
        })
    };
    wait_until(|| logon_count(&client.sent.lock().unwrap()) >= 2)
        .await
        .expect("the retried authentication should send its logon");
    let (initial_logon_id, retried_logon_id) = {
        let sent = client.sent.lock().unwrap();
        let mut logons = sent.iter().filter(|message| {
            matches!(
                message.params.unsigned.method_name(),
                BinanceWebsocketMethodName::Logon
            )
        });
        (
            logons.next().expect("initial logon").id.clone(),
            logons.next().expect("retried logon").id.clone(),
        )
    };
    assert_ne!(
        initial_logon_id, retried_logon_id,
        "each authentication attempt must use a fresh logon id"
    );

    retried_send.await.expect("send task should not panic");

    // No logon response leaked to the user's listener: the failed send
    // never attempted a logon for the dead connection, and the retried
    // logon was consumed by its own waiter.
    assert!(
        received.lock().unwrap().is_empty(),
        "logon responses must not leak to the delegate listener"
    );
}

#[tokio::test]
async fn sync_clock_syncs_the_server_clock() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let clock = Clock::default();
    let connector = mock_session_connector(client_tx, None, None, IgnoreListener, clock).unwrap();
    let client = client_rx.recv().unwrap();

    // Connect establishes the transport only: clock syncing is
    // user-invoked, so nothing is sent and the clock still needs a sync.
    connector.connect().await.expect("connect should succeed");
    assert!(client.sent.lock().unwrap().is_empty());

    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");

    // The sync clock message is the unsigned `time` request.
    let sent = client.sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "sync clock");
    assert!(matches!(
        sent[0].params.unsigned.method_name(),
        BinanceWebsocketMethodName::Time
    ));
    assert!(
        sent[0].params.signature.is_none(),
        "the time request must be unsigned"
    );
}

#[tokio::test]
async fn sync_clock_syncs_the_server_clock_from_a_fresh_time_request() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let clock = Clock::default();
    let connector = mock_session_connector(client_tx, None, None, IgnoreListener, clock).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert_eq!(client.sent.lock().unwrap().len(), 1, "logon");

    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
    let sent = client.sent.lock().unwrap();
    assert_eq!(sent.len(), 2, "logon + sync_clock");
    // The sync is a fresh unsigned time request, matched by its own id
    // rather than tied to the authentication logon.
    let synchronization = &sent[1];
    assert!(
        matches!(
            synchronization.params.unsigned.method_name(),
            BinanceWebsocketMethodName::Time
        ),
        "sync_clock must send a time request"
    );
    assert!(
        synchronization.params.signature.is_none(),
        "the sync_clock request must be unsigned"
    );
}
