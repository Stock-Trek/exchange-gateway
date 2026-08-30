use crate::{
    clock::Clock,
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
    time::BinanceTimeResult,
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketResponseResult, BinanceWebsocketUnsignedParams,
        BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
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
    clock: Arc<Clock>,
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
        match message.metadata.method {
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
                    Some(error) => logon_response(message.metadata.id, 401, Some(error.clone())),
                    None => logon_response(message.metadata.id, 200, None),
                };
                self.listener.on_message(response).await?;
            }
            BinanceWebsocketMethodName::Time => {
                let response = BinanceWebsocketResponse {
                    error: None,
                    id: message.metadata.id,
                    rateLimits: vec![],
                    result: Some(BinanceWebsocketResponseResult::Time(BinanceTimeResult {
                        serverTime: self.clock.now_millis(),
                    })),
                    status: 200,
                };
                self.listener.on_message(response).await?;
            }
            _ => {}
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
    clock: Arc<Clock>,
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
    let clock_for_factory = clock.clone();
    let client_factory =
        move |websocket_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>| {
            let mock_client = MockWebsocketClient {
                listener: websocket_listener,
                connected: Arc::new(AtomicBool::new(false)),
                sent: Arc::new(Mutex::new(Vec::new())),
                logon_gate,
                logon_error,
                clock: clock_for_factory,
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
        clock,
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
async fn post_logon_requests_omit_api_key_and_signature() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = mock_session_connector(
        client_tx,
        None,
        None,
        Duration::from_secs(20),
        Arc::new(IgnoreListener),
        Arc::new(Clock::default()),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    connector.authenticate().await.expect("should succeed");
    assert!(connector.is_authenticated().unwrap());
    connector
        .send(order_request(), true, Duration::from_secs(5))
        .await
        .expect("send should succeed");

    // After session.logon the connection is authenticated, so the order must
    // omit both apiKey and signature: apiKey without signature is an
    // undocumented combination and is rejected (-1022).
    let sent = client.sent.lock().unwrap();
    let order = sent
        .iter()
        .find(|message| {
            matches!(
                message.metadata.method,
                BinanceWebsocketMethodName::PlaceOrder
            )
        })
        .expect("the order should have been sent");
    assert!(order.params.signature.is_none());
    let BinanceWebsocketUnsignedParams::SpotOrderRequest(params) = &order.params.params else {
        panic!("expected a spot order request");
    };
    assert!(
        params.apiKey.is_none(),
        "post-logon requests must omit apiKey"
    );
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
            Arc::new(Clock::default()),
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    connector
        .authenticate()
        .await
        .expect("authentication should succeed");
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

    // A signed send while the connection is down must fail fast with
    // `NotAuthenticated`: authentication is user-invoked rather than
    // automatic, so no re-authentication logon is attempted while the
    // connection is down and the order is never queued under a dead
    // session.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(order_request(), true, Duration::from_secs(5)),
    )
    .await
    .expect("send while the connection is down should fail fast, not hang")
    .expect_err("send must fail while the connection is down");
    assert!(matches!(error, EGError::NotAuthenticated));
    // Neither the logon nor the order was ever recorded: the fail-fast
    // happens before any message reaches the transport.
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);
    assert!(!client.sent.lock().unwrap().iter().any(|message| matches!(
        message.metadata.method,
        BinanceWebsocketMethodName::PlaceOrder
    )));
    assert!(!connector.is_authenticated().unwrap());

    // Once the connection comes back, the user re-authenticates and the
    // signed send goes out normally.
    client.connect().await.expect("reconnect should succeed");
    connector
        .authenticate()
        .await
        .expect("authentication should succeed");
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
        Arc::new(Clock::default()),
    )
    .unwrap();

    connector.connect().await.expect("connect should succeed");
    // Authentication is user-invoked, not automatic: the logon it sends
    // consumes 2 of the 6000 weight budget.
    connector
        .authenticate()
        .await
        .expect("authentication should succeed");

    // exchangeInfo costs 20, so exactly 299 requests fit in the remaining
    // 5998. If the logon weight were not counted, a 1500th request would
    // still fit.
    for _ in 0..299 {
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
async fn rejected_logon_fails_authentication_and_does_not_leak_to_listener() {
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
        Arc::new(Clock::default()),
    )
    .unwrap();

    // Connect only establishes the transport; authentication is
    // user-invoked, so the rejected logon surfaces from `authenticate` as
    // the exchange's actual error (not EGError::TimedOut after the full
    // 20 s logon timeout) and fails promptly.
    connector.connect().await.expect("connect should succeed");
    let error = tokio::time::timeout(Duration::from_secs(1), connector.authenticate())
        .await
        .expect("rejected logon should fail quickly, not time out")
        .expect_err("authentication should fail");
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
            Arc::new(Clock::default()),
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    connector
        .authenticate()
        .await
        .expect("authentication should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // Simulate the connection dropping and the iris client reconnecting.
    client.connected.store(false, Ordering::SeqCst);
    client.connect().await.expect("reconnect should succeed");
    assert!(!connector.is_authenticated().unwrap());

    // Hold logon responses so the next re-authentication stays in flight.
    logon_gate.block.store(true, Ordering::SeqCst);

    // Authentication is user-invoked, so each concurrent signed send
    // re-authenticates before sending. The first one starts the
    // re-authentication logon and blocks on the held logon response; the
    // others wait for the in-flight attempt instead of starting a second
    // one.
    let sends = (0..3)
        .map(|_| {
            let connector = connector.clone();
            tokio::spawn(async move {
                connector.authenticate().await?;
                connector
                    .send(order_request(), true, Duration::from_secs(5))
                    .await
            })
        })
        .collect::<Vec<_>>();
    wait_until(|| logon_count(&client.sent.lock().unwrap()) == 2)
        .await
        .expect("re-authentication logon should be in flight");
    assert_eq!(
        logon_count(&client.sent.lock().unwrap()),
        2,
        "re-authentication logon should be in flight"
    );
    // Give the concurrent sends a chance to wrongly start a second
    // authentication by driving the clock forward instead of sleeping.
    with_paused_clock(async {
        for _ in 0..10 {
            tick(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        logon_count(&client.sent.lock().unwrap()),
        2,
        "concurrent sends must not start a second authentication"
    );

    // Release the in-flight logon: all three sends should now complete.
    logon_gate.block.store(false, Ordering::SeqCst);
    logon_gate.release.notify_one();

    for send in sends {
        send.await
            .expect("send task should not panic")
            .expect("send should succeed");
    }

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
async fn logon_sent_while_reconnecting_fails_fast_and_leaves_nothing_pending() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let received = Arc::new(Mutex::new(Vec::new()));
    let listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
        Arc::new(RecordingListener {
            received: received.clone(),
        });
    let connector = Arc::new(
        mock_session_connector(
            client_tx,
            None,
            None,
            Duration::from_secs(20),
            listener,
            Arc::new(Clock::default()),
        )
        .unwrap(),
    );
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    connector
        .authenticate()
        .await
        .expect("authentication should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

    // The connection drops and the session goes stale.
    client
        .disconnect()
        .await
        .expect("disconnect should succeed");
    assert!(!connector.is_authenticated().unwrap());

    // A signed send while the connection is down must fail fast with
    // `NotAuthenticated`: authentication is user-invoked, so no logon is
    // attempted for the dead connection and nothing is left queued to
    // resolve (or confuse) a later authentication.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(order_request(), true, Duration::from_secs(5)),
    )
    .await
    .expect("send while reconnecting should fail fast, not hang until its timeout")
    .expect_err("the stale session must fail the send");
    assert!(matches!(error, EGError::NotAuthenticated));
    assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);
    assert!(!connector.is_authenticated().unwrap());

    // The connection comes back and the session is stale again, so the
    // user's next authentication sends a fresh logon with an id distinct
    // from the initial one, and the signed send goes out normally.
    client.connect().await.expect("reconnect should succeed");
    let retried_send = {
        let connector = connector.clone();
        tokio::spawn(async move {
            connector
                .authenticate()
                .await
                .expect("authentication should succeed");
            connector
                .send(order_request(), true, Duration::from_secs(5))
                .await
                .expect("the retried send should succeed against its own logon")
        })
    };
    wait_until(|| logon_count(&client.sent.lock().unwrap()) >= 2)
        .await
        .expect("the retried authentication should send its logon");
    let (initial_logon_id, retried_logon_id) = {
        let sent = client.sent.lock().unwrap();
        let mut logons = sent
            .iter()
            .filter(|message| matches!(message.metadata.method, BinanceWebsocketMethodName::Logon));
        (
            logons.next().expect("initial logon").metadata.id.clone(),
            logons.next().expect("retried logon").metadata.id.clone(),
        )
    };
    assert_ne!(
        initial_logon_id, retried_logon_id,
        "each authentication attempt must use a fresh logon id"
    );

    retried_send.await.expect("send task should not panic");
    assert!(connector.is_authenticated().unwrap());

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
    let clock = Arc::new(Clock::default());
    let connector = mock_session_connector(
        client_tx,
        None,
        None,
        Duration::from_secs(20),
        Arc::new(IgnoreListener),
        clock.clone(),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    // Connect establishes the transport only: clock syncing is
    // user-invoked, so nothing is sent and the clock still needs a sync.
    connector.connect().await.expect("connect should succeed");
    assert!(client.sent.lock().unwrap().is_empty());
    assert!(clock.should_sync(), "connect must not sync the clock");

    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
    assert!(
        !clock.should_sync(),
        "sync_clock must refresh the clock sync time"
    );

    // The sync clock message is the unsigned `time` request.
    let sent = client.sent.lock().unwrap();
    assert_eq!(sent.len(), 1, "sync clock");
    assert!(matches!(
        sent[0].metadata.method,
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
    let clock = Arc::new(Clock::default());
    let connector = mock_session_connector(
        client_tx,
        None,
        None,
        Duration::from_secs(20),
        Arc::new(IgnoreListener),
        clock.clone(),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    // Authentication is user-invoked, not automatic.
    connector
        .authenticate()
        .await
        .expect("authentication should succeed");
    assert!(connector.is_authenticated().unwrap());
    assert_eq!(client.sent.lock().unwrap().len(), 1, "logon");

    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
    let sent = client.sent.lock().unwrap();
    assert_eq!(sent.len(), 2, "logon + sync_clock");
    // The sync is a fresh unsigned time request, matched by its own id
    // rather than tied to the authentication logon.
    let sync_clock = &sent[1];
    assert!(
        matches!(sync_clock.metadata.method, BinanceWebsocketMethodName::Time),
        "sync_clock must send a time request"
    );
    assert!(
        sync_clock.params.signature.is_none(),
        "the sync_clock request must be unsigned"
    );
}
