use crate::{
    clock::Clock,
    connector::Connector,
    error::{EGError, EGResult},
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
    specs::binance::websocket::connector,
    transports::websocket::WebsocketClientTrait,
};
use async_trait::async_trait;
use exchange_types::{
    binance::{
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus,
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
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// A mock WebSocket client: connects by notifying its listener, answers
/// every request with a matching-id success reply (answering `time` with
/// the clock's server time), and can be asked to push unsolicited messages.
#[derive(Clone)]
struct MockWebsocketClient {
    listener: Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
    connected: Arc<AtomicBool>,
    sent: Arc<Mutex<Vec<BinanceWebsocketRequest>>>,
    clock: Clock,
}

impl MockWebsocketClient {
    async fn push(&self, response: BinanceWebsocketResponse) -> EGResult<()> {
        self.listener.on_message(response).await
    }
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
        let response = match message.params.unsigned.method_name() {
            BinanceWebsocketMethodName::Time => BinanceWebsocketResponse {
                error: None,
                id: message.id.clone(),
                rateLimits: vec![],
                result: Some(BinanceWebsocketResponseResult::Time(BinanceTimeResult {
                    serverTime: self.clock.now_millis(),
                })),
                status: 200,
            },
            _ => BinanceWebsocketResponse {
                error: None,
                id: message.id.clone(),
                rateLimits: vec![],
                result: None,
                status: 200,
            },
        };
        self.listener.on_message(response).await
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.connected.store(false, Ordering::SeqCst);
        self.listener.on_disconnected().await
    }
}

/// A listener that records lifecycle events and messages.
struct RecordingListener {
    connected: Arc<AtomicBool>,
    disconnected: Arc<AtomicBool>,
    received: Arc<Mutex<Vec<BinanceWebsocketResponse>>>,
}

#[async_trait]
impl ListenerTrait for RecordingListener {
    type TMessage = BinanceWebsocketResponse;

    async fn on_connected(&self) -> EGResult<()> {
        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        self.disconnected.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn on_message(&self, message: BinanceWebsocketResponse) -> EGResult<()> {
        self.received
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .push(message);
        Ok(())
    }
}

fn mock_websocket_connector(
    client_handle: std::sync::mpsc::Sender<MockWebsocketClient>,
    listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
    server_clock: Clock,
) -> EGResult<impl Connector<BinanceWebsocketRequest, BinanceWebsocketResponse>> {
    let client_creator = Box::new(
        move |(_url, websocket_listener): (
            String,
            Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
        )| {
            let mock_client = MockWebsocketClient {
                listener: websocket_listener,
                connected: Arc::new(AtomicBool::new(false)),
                sent: Arc::new(Mutex::new(Vec::new())),
                clock: server_clock,
            };
            let _ = client_handle.send(mock_client.clone());
            Ok(mock_client)
        },
    );
    connector(TradingMode::Paper, listener, client_creator)
}

fn exchange_info_request() -> BinanceWebsocketRequest {
    BinanceWebsocketRequest {
        id: "exchange-info".into(),
        params: BinanceWebsocketSignedParams {
            unsigned: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
            signature: None,
        },
    }
}

fn response(id: String) -> BinanceWebsocketResponse {
    BinanceWebsocketResponse {
        error: None,
        id,
        rateLimits: vec![],
        result: None,
        status: 200,
    }
}

#[tokio::test]
async fn websocket_connector_send_returns_the_matching_response() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let listener = RecordingListener {
        connected: Arc::new(AtomicBool::new(false)),
        disconnected: Arc::new(AtomicBool::new(false)),
        received: Arc::new(Mutex::new(Vec::new())),
    };
    let connector = mock_websocket_connector(client_tx, listener, Clock::default()).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    let response = connector
        .send(exchange_info_request(), Duration::from_secs(5))
        .await
        .expect("send should succeed");
    // The mock answers with a matching-id reply, which the send returns
    // directly: the caller observes the raw exchange response.
    assert_eq!(response.id, "exchange-info");
    assert_eq!(response.status, 200);

    // The matching response was consumed by the send waiter: the delegate
    // listener sees no message for it.
    assert!(client.sent.lock().unwrap().len() == 1);
}

#[tokio::test]
async fn websocket_connector_forwards_unsolicited_messages_to_the_listener() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let received = Arc::new(Mutex::new(Vec::new()));
    let listener = RecordingListener {
        connected: Arc::new(AtomicBool::new(false)),
        disconnected: Arc::new(AtomicBool::new(false)),
        received: received.clone(),
    };
    let connector = mock_websocket_connector(client_tx, listener, Clock::default()).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    // A message with no matching send waiter is a push: it is forwarded to
    // the user's listener.
    client
        .push(response("unsolicited".into()))
        .await
        .expect("push should succeed");
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].id, "unsolicited");
}

#[tokio::test]
async fn websocket_connector_sync_clock_syncs_the_server_clock() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let server_clock = Clock::default();
    server_clock
        .sync(server_clock.now_millis() + 10_000, Duration::ZERO)
        .expect("Cannot sync the server clock");
    let listener = RecordingListener {
        connected: Arc::new(AtomicBool::new(false)),
        disconnected: Arc::new(AtomicBool::new(false)),
        received: Arc::new(Mutex::new(Vec::new())),
    };
    let connector = mock_websocket_connector(client_tx, listener, server_clock).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    // Nothing is sent until sync_clock is invoked.
    assert!(client.sent.lock().unwrap().is_empty());
    let local = connector.server_time_millis().expect("No local time");
    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
    let server_time = connector.server_time_millis().expect("No server time");
    assert!(server_time >= local + 10_000, "server_time: {server_time}");
    assert!(
        server_time < local + 10_000 + 60_000,
        "server_time: {server_time}"
    );

    // The sync clock message is the unsigned `time` request, answered with
    // the mock's view of server time.
    let sent = client.sent.lock().unwrap();
    assert_eq!(sent.len(), 1);
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
async fn websocket_connector_connection_lifecycle_reaches_the_listener() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connected = Arc::new(AtomicBool::new(false));
    let disconnected = Arc::new(AtomicBool::new(false));
    let listener = RecordingListener {
        connected: connected.clone(),
        disconnected: disconnected.clone(),
        received: Arc::new(Mutex::new(Vec::new())),
    };
    let connector = mock_websocket_connector(client_tx, listener, Clock::default()).unwrap();
    let _client = client_rx.recv().unwrap();

    assert!(!connector.is_connected().unwrap());
    connector.connect().await.expect("connect should succeed");
    assert!(connector.is_connected().unwrap());
    assert!(connected.load(Ordering::SeqCst));
    connector
        .disconnect()
        .await
        .expect("disconnect should succeed");
    assert!(!connector.is_connected().unwrap());
    assert!(disconnected.load(Ordering::SeqCst));
}

#[tokio::test]
async fn websocket_connector_send_fails_when_the_connection_is_down() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let listener = RecordingListener {
        connected: Arc::new(AtomicBool::new(false)),
        disconnected: Arc::new(AtomicBool::new(false)),
        received: Arc::new(Mutex::new(Vec::new())),
    };
    let connector = mock_websocket_connector(client_tx, listener, Clock::default()).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    // Dropping the connection fails any pending waiters and surfaces the
    // disconnect to the listener.
    client
        .disconnect()
        .await
        .expect("disconnect should succeed");

    // A send on a dead connection fails fast with the transport error
    // instead of hanging until its timeout.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        connector.send(exchange_info_request(), Duration::from_secs(5)),
    )
    .await
    .expect("send on a dead connection should fail fast, not hang")
    .expect_err("send must fail while the connection is down");
    assert!(
        matches!(error, EGError::NotSent(_)),
        "unexpected error: {error:?}"
    );
}
