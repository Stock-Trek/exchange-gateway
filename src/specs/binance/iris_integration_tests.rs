use crate::{
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::ArcTryConvertValue,
    listeners::listener::ListenerTrait,
    specs::binance::websocket::connector_with_client_factory,
    transports::websocket::WebsocketClientTrait,
};
use async_trait::async_trait;
use exchange_types::binance::{
    error::BinanceError,
    exchange_info::{
        BinanceExchangeInfoParams, BinanceExchangeInfoPermission, BinanceExchangeInfoSymbolStatus,
        BinanceOrderType,
    },
    spot::{
        BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
        BinanceSpotOrderParams, BinanceTimeInForce,
    },
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketUnsignedParams, BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
use std::{
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
struct MockWebsocketClient {
    listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
    connected: Arc<AtomicBool>,
    sent: Arc<Mutex<Vec<BinanceWebsocketRequest>>>,
    logon_gate: Option<LogonGate>,
    logon_error: Option<BinanceError>,
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
        if matches!(message.metadata.method, BinanceWebsocketMethodName::Logon) {
            if let Some(gate) = &self.logon_gate {
                if gate.fail.load(Ordering::SeqCst) {
                    return Err(EGError::TimedOut);
                }
                if gate.block.load(Ordering::SeqCst) {
                    gate.release.notified().await;
                }
            }
            let response = match &self.logon_error {
                Some(error) => logon_response(message.metadata.id, 401, Some(error.clone())),
                None => logon_response(message.metadata.id, 200, None),
            };
            self.listener.on_message(response).await?;
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
    logon_timeout: Duration,
    listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
) -> EGResult<impl Connector<BinanceWebsocketUnsignedRequest, BinanceWebsocketResponse>> {
    let credentials = ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    };
    let to_unsigned_request: ArcTryConvertValue<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketUnsignedRequest,
    > = Arc::new(Ok);
    let to_external_response: ArcTryConvertValue<
        BinanceWebsocketResponse,
        BinanceWebsocketResponse,
    > = Arc::new(Ok);
    // The production connector builds the internal response listener and
    // hands it to the client factory, so the scripted client is wired into
    // the same listener (and shared auth gate) the transport uses.
    let client_factory =
        move |websocket_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>| {
            let mock_client = MockWebsocketClient {
                listener: websocket_listener,
                connected: Arc::new(AtomicBool::new(false)),
                sent: Arc::new(Mutex::new(Vec::new())),
                logon_gate,
                logon_error,
            };
            let _ = client_handle.send(mock_client.clone());
            let client: Arc<
                dyn WebsocketClientTrait<
                        TransportReq = BinanceWebsocketRequest,
                        TransportRes = BinanceWebsocketResponse,
                    >,
            > = Arc::new(mock_client);
            client
        };
    connector_with_client_factory(
        client_factory,
        logon_timeout,
        to_unsigned_request,
        to_external_response,
        listener,
        Some(credentials),
        true,
    )
}

fn order_request() -> BinanceWebsocketUnsignedRequest {
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id: "order-1".into(),
            method: BinanceWebsocketMethodName::PlaceOrder,
        },
        params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
    }
}

fn exchange_info_request() -> BinanceWebsocketUnsignedRequest {
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id: "exchange-info".into(),
            method: BinanceWebsocketMethodName::ExchangeInfo,
        },
        params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        }),
    }
}

fn logon_count(sent: &[BinanceWebsocketRequest]) -> usize {
    sent.iter()
        .filter(|message| matches!(message.metadata.method, BinanceWebsocketMethodName::Logon))
        .count()
}

/// Polls `condition` until it holds, with a generous deadline. The runtime
/// clock is driven forward with `pause`/`advance` instead of sleeping so
/// timers fire without spending wall-clock time; it is resumed before
/// returning so the rest of the test runs on real time.
async fn wait_until(mut condition: impl FnMut() -> bool) -> Option<()> {
    tokio::time::pause();
    let result = async {
        for _ in 0..500 {
            if condition() {
                return Some(());
            }
            // Fire any timers due in the next window and let the connector's
            // tasks run; the mock transport itself is in-memory.
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }
        None
    }
    .await;
    tokio::time::resume();
    result
}

#[tokio::test]
async fn reauthenticates_after_reconnect() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = mock_session_connector(
        client_tx,
        None,
        None,
        Duration::from_secs(20),
        Arc::new(IgnoreListener),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // A signed request on a live connection does not re-authenticate.
    connector
        .send(order_request(), true, Duration::from_secs(5))
        .await
        .expect("send should succeed");
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // Simulate the connection dropping and the iris client reconnecting.
    client.connected.store(false, Ordering::SeqCst);
    client.connect().await.expect("reconnect should succeed");

    // The old session is stale until re-authentication runs.
    assert!(!connector.is_authenticated().unwrap());

    // The next signed send re-authenticates before sending.
    connector
        .send(order_request(), true, Duration::from_secs(5))
        .await
        .expect("send should succeed");
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 2);
    assert!(connector.is_authenticated().unwrap());
    assert!(client.sent.lock().unwrap().iter().any(|message| matches!(
        message.metadata.method,
        BinanceWebsocketMethodName::PlaceOrder
    )));
}

#[tokio::test]
async fn sends_during_a_drop_fail_fast_until_reconnect() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = Arc::new(
        mock_session_connector(
            client_tx,
            None,
            None,
            Duration::from_secs(20),
            Arc::new(IgnoreListener),
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // The connection drops. The transport reports the disconnect (as iris
    // does when the socket closes, before it reconnects), bumping the
    // connection epoch so the session is stale while the connection is
    // down and re-authentication cannot be bypassed.
    client
        .disconnect()
        .await
        .expect("disconnect should succeed");
    assert!(!connector.is_authenticated().unwrap());

    // A signed send while the connection is down must fail fast: iris
    // rejects sends with `ConnectionClosed` as soon as the connected flag
    // is down, so the re-authentication logon never goes out and the
    // order is never queued under a dead session. (Before the fix the
    // stale check saw the old epoch, skipped re-auth, and queued the
    // order under a dead session.)
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(order_request(), true, Duration::from_secs(5)),
    )
    .await
    .expect("send while the connection is down should fail fast, not hang")
    .expect_err("send must fail while the connection is down");
    assert!(matches!(
        &error,
        EGError::External(e)
            if e
                .downcast_ref::<iris::ConnectionError>()
                .is_some_and(|error| {
                    matches!(error, iris::ConnectionError::ConnectionClosed)
                })
    ));
    // Neither the logon nor the order was ever recorded: the fail-fast
    // happens before any message reaches the transport.
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);
    assert!(!client.sent.lock().unwrap().iter().any(|message| matches!(
        message.metadata.method,
        BinanceWebsocketMethodName::PlaceOrder
    )));
    assert!(!connector.is_authenticated().unwrap());

    // Once the connection comes back, the next signed send
    // re-authenticates and goes out normally.
    client.connect().await.expect("reconnect should succeed");
    connector
        .send(order_request(), true, Duration::from_secs(5))
        .await
        .expect("send should succeed once the connection returns");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 2);
    assert!(client.sent.lock().unwrap().iter().any(|message| matches!(
        message.metadata.method,
        BinanceWebsocketMethodName::PlaceOrder
    )));
}

#[tokio::test]
async fn logon_weight_counts_against_weight_rate_limit() {
    let (client_tx, _client_rx) = std::sync::mpsc::channel();
    let connector = mock_session_connector(
        client_tx,
        None,
        None,
        Duration::from_secs(20),
        Arc::new(IgnoreListener),
    )
    .unwrap();

    connector.connect().await.expect("connect should succeed");

    // The logon consumes 2 of the 6000 weight budget; exchangeInfo costs 4,
    // so exactly 1499 more requests fit in the remaining 5998. If the logon
    // weight were not counted, a 1500th request would still fit.
    for _ in 0..1499 {
        connector
            .send(exchange_info_request(), false, Duration::from_secs(5))
            .await
            .expect("send should succeed");
    }
    let result = connector
        .send(exchange_info_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
}

#[tokio::test]
async fn rejected_logon_fails_connect_and_does_not_leak_to_listener() {
    let (client_tx, _client_rx) = std::sync::mpsc::channel();
    let received = Arc::new(Mutex::new(Vec::new()));
    let listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
        Arc::new(RecordingListener {
            received: received.clone(),
        });
    let connector = mock_session_connector(
        client_tx,
        None,
        Some(BinanceError {
            code: -2014,
            msg: "API-key format invalid.".into(),
        }),
        Duration::from_secs(20),
        listener,
    )
    .unwrap();

    // The rejected logon must surface as the exchange's actual error
    // (not EGError::TimedOut after the full 20 s logon timeout) and fail
    // promptly.
    let error = tokio::time::timeout(Duration::from_secs(1), connector.connect())
        .await
        .expect("rejected logon should fail quickly, not time out")
        .expect_err("connect should fail");
    match error {
        EGError::ApiError { code, message } => {
            assert_eq!(code, -2014);
            assert_eq!(message, "API-key format invalid.");
        }
        other => panic!("expected ApiError, got: {other:?}"),
    }
    // The internal session.logon rejection must not be forwarded to the
    // user's delegate listener.
    assert!(
        received.lock().unwrap().is_empty(),
        "rejected logon must not leak to the delegate listener"
    );
}

#[tokio::test]
async fn concurrent_sends_wait_for_in_flight_authentication() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let logon_gate = LogonGate {
        block: Arc::new(AtomicBool::new(false)),
        release: Arc::new(tokio::sync::Notify::new()),
        fail: Arc::new(AtomicBool::new(false)),
    };
    let connector = Arc::new(
        mock_session_connector(
            client_tx,
            Some(logon_gate.clone()),
            None,
            Duration::from_secs(20),
            Arc::new(IgnoreListener),
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // Simulate the connection dropping and the iris client reconnecting.
    client.connected.store(false, Ordering::SeqCst);
    client.connect().await.expect("reconnect should succeed");
    assert!(!connector.is_authenticated().unwrap());

    // Hold logon responses so the next re-authentication stays in flight.
    logon_gate.block.store(true, Ordering::SeqCst);

    // The first signed send starts re-authentication and blocks on the
    // held logon response.
    let first = {
        let connector = connector.clone();
        tokio::spawn(async move {
            connector
                .send(order_request(), true, Duration::from_secs(5))
                .await
        })
    };
    wait_until(|| logon_count(&client.sent.lock().unwrap()) == 2)
        .await
        .expect("re-authentication logon should be in flight");

    // Two more signed sends arrive while authentication is in flight:
    // they must wait for it to finish instead of starting a second one.
    let second = {
        let connector = connector.clone();
        tokio::spawn(async move {
            connector
                .send(order_request(), true, Duration::from_secs(5))
                .await
        })
    };
    let third = {
        let connector = connector.clone();
        tokio::spawn(async move {
            connector
                .send(order_request(), true, Duration::from_secs(5))
                .await
        })
    };
    // Give the concurrent sends plenty of scheduling turns to (wrongly)
    // start a second authentication, driving the clock forward with
    // `pause`/`advance` instead of sleeping so any timers involved fire
    // without waiting real time.
    tokio::time::pause();
    for _ in 0..10 {
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
    }
    tokio::time::resume();
    assert_eq!(
        logon_count(&client.sent.lock().unwrap()),
        2,
        "concurrent sends must not start a second authentication"
    );

    // Release the in-flight logon: all three sends should now complete.
    logon_gate.block.store(false, Ordering::SeqCst);
    logon_gate.release.notify_one();

    first
        .await
        .expect("first send task should not panic")
        .expect("first send should succeed");
    second
        .await
        .expect("second send task should not panic")
        .expect("second send should succeed");
    third
        .await
        .expect("third send task should not panic")
        .expect("third send should succeed");

    assert_eq!(logon_count(&client.sent.lock().unwrap()), 2);
    assert!(connector.is_authenticated().unwrap());
    let order_count = client
        .sent
        .lock()
        .unwrap()
        .iter()
        .filter(|message| {
            matches!(
                message.metadata.method,
                BinanceWebsocketMethodName::PlaceOrder
            )
        })
        .count();
    assert_eq!(order_count, 3);
}

#[tokio::test]
async fn failed_reauthentication_keeps_the_session_stale() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let logon_gate = LogonGate {
        block: Arc::new(AtomicBool::new(false)),
        release: Arc::new(tokio::sync::Notify::new()),
        fail: Arc::new(AtomicBool::new(false)),
    };
    let connector = Arc::new(
        mock_session_connector(
            client_tx,
            Some(logon_gate.clone()),
            None,
            Duration::from_secs(20),
            Arc::new(IgnoreListener),
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(connector.is_authenticated().unwrap());

    // Simulate the connection dropping and the iris client reconnecting.
    client.connected.store(false, Ordering::SeqCst);
    client.connect().await.expect("reconnect should succeed");
    assert!(!connector.is_authenticated().unwrap());

    // Make the next logon fail (e.g. expired/bad key or a transient
    // timeout).
    logon_gate.fail.store(true, Ordering::SeqCst);

    // The failed re-authentication must not mark the session as
    // authenticated: the connector stays unauthenticated and the next
    // signed send retries the logon instead of going out on a connection
    // that was never logged in.
    let error = connector
        .send(order_request(), true, Duration::from_secs(5))
        .await
        .expect_err("send must fail when re-authentication fails");
    assert!(matches!(error, EGError::TimedOut));
    assert!(!connector.is_authenticated().unwrap());

    // Once the logon succeeds again, the next signed send re-authenticates
    // and goes out normally.
    logon_gate.fail.store(false, Ordering::SeqCst);
    connector
        .send(order_request(), true, Duration::from_secs(5))
        .await
        .expect("send should succeed once re-authentication succeeds");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 3);
    assert!(client.sent.lock().unwrap().iter().any(|message| matches!(
        message.metadata.method,
        BinanceWebsocketMethodName::PlaceOrder
    )));
}

#[tokio::test]
async fn logon_sent_while_reconnecting_fails_fast_and_leaves_nothing_pending() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let logon_gate = LogonGate {
        block: Arc::new(AtomicBool::new(false)),
        release: Arc::new(tokio::sync::Notify::new()),
        fail: Arc::new(AtomicBool::new(false)),
    };
    let received = Arc::new(Mutex::new(Vec::new()));
    let listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
        Arc::new(RecordingListener {
            received: received.clone(),
        });
    // A short logon waiter: if the reconnecting logon were buffered for
    // the fresh connection it would time out after 50 ms, but fail-fast
    // rejects it immediately, so the waiter never gets a chance to fire.
    let connector = Arc::new(
        mock_session_connector(
            client_tx,
            Some(logon_gate.clone()),
            None,
            Duration::from_millis(50),
            listener,
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // The connection drops and the session goes stale.
    client
        .disconnect()
        .await
        .expect("disconnect should succeed");
    assert!(!connector.is_authenticated().unwrap());

    // A signed send while the connection is down must fail fast: iris
    // rejects the logon with `ConnectionClosed` instead of buffering it
    // for the fresh connection, so the send fails immediately rather than
    // waiting out its logon timeout, and no logon is left queued to
    // resolve (or confuse) a later authentication.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(order_request(), true, Duration::from_secs(5)),
    )
    .await
    .expect("send while reconnecting should fail fast, not hang until its timeout")
    .expect_err("the failed logon must fail the send");
    assert!(matches!(
        &error,
        EGError::External(e)
            if e
                .downcast_ref::<iris::ConnectionError>()
                .is_some_and(|error| {
                    matches!(error, iris::ConnectionError::ConnectionClosed)
                })
    ));
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);
    assert!(!connector.is_authenticated().unwrap());

    // The connection comes back and the session is stale again, so the
    // next signed send starts a fresh authentication attempt with a logon
    // id distinct from the initial one.
    client.connect().await.expect("reconnect should succeed");
    let retried_send = {
        let connector = connector.clone();
        tokio::spawn(async move {
            connector
                .send(order_request(), true, Duration::from_secs(5))
                .await
        })
    };
    wait_until(|| logon_count(&client.sent.lock().unwrap()) >= 2)
        .await
        .expect("the retried authentication should send its logon");
    let (initial_logon_id, retried_logon_id) = {
        let sent = client.sent.lock().unwrap();
        (sent[0].metadata.id.clone(), sent[1].metadata.id.clone())
    };
    assert_ne!(
        initial_logon_id, retried_logon_id,
        "each authentication attempt must use a fresh logon id"
    );

    // The retried attempt's own logon response resolves its waiter and
    // the send completes normally.
    retried_send
        .await
        .expect("send task should not panic")
        .expect("the retried send should succeed against its own logon");
    assert!(connector.is_authenticated().unwrap());

    // No logon response leaked to the user's listener: the reconnecting
    // logon was never accepted (so it has no response to deliver), and
    // the retried logon was consumed by its own waiter.
    assert!(
        received.lock().unwrap().is_empty(),
        "logon responses must not leak to the delegate listener"
    );
}
