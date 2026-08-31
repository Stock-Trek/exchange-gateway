use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
    transports::websocket::WebsocketClientTrait,
};
use async_trait::async_trait;
use futures_timer::Delay;
use iris::{Client as IrisClient, Config as IrisConfig, Listener as IrisListener};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

pub(crate) struct IrisWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + 'static,
    TransportRes: DeserializeOwned + Send + 'static,
{
    client: IrisClient<TransportReq, TransportRes>,
}

impl<TransportReq, TransportRes> IrisWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + 'static,
    TransportRes: DeserializeOwned + Send + 'static,
{
    pub(crate) fn with_config(
        url: &str,
        config: IrisConfig,
        listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
    ) -> Self {
        let client = IrisClient::new(
            config,
            Arc::new(IrisListenerAdapter { delegate: listener }),
            url,
        );
        Self { client }
    }

    async fn send_message_with_delay<D>(&self, message: TransportReq, delay: D) -> EGResult<()>
    where
        D: Future<Output = ()> + Send + 'static,
    {
        let mut send = Box::pin(self.client.send(message));
        let mut delay = Box::pin(delay);
        poll_fn(move |cx| match send.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(|e| EGError::External(Box::new(e)))),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
        .await
    }
}

#[async_trait]
impl<TransportReq, TransportRes> WebsocketClientTrait
    for IrisWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + 'static,
    TransportRes: DeserializeOwned + Send + 'static,
{
    type TransportReq = TransportReq;
    type TransportRes = TransportRes;

    async fn connect(&self) -> EGResult<()> {
        self.client
            .connect()
            .await
            .map_err(|e| EGError::External(Box::new(e)))
    }

    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    async fn send_message(&self, message: Self::TransportReq, timeout: Duration) -> EGResult<()> {
        self.send_message_with_delay(message, Delay::new(timeout))
            .await
    }

    async fn disconnect(&self) -> EGResult<()> {
        self.client
            .disconnect()
            .await
            .map_err(|e| EGError::External(Box::new(e)))
    }
}

impl<TransportReq, TransportRes> std::fmt::Debug for IrisWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + Sync + 'static,
    TransportRes: DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisWebsocketClient")
            .field("client", &"<websocket::WebsocketClient>")
            .finish()
    }
}

struct IrisListenerAdapter<TransportRes> {
    delegate: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
}

#[async_trait]
impl<TransportRes> IrisListener<TransportRes> for IrisListenerAdapter<TransportRes>
where
    TransportRes: DeserializeOwned + Send + 'static,
{
    async fn on_connected(&self) {
        if let Err(error) = self.delegate.on_connected().await {
            let _ = self.delegate.on_error(error).await;
        }
    }
    async fn on_disconnected(&self) {
        if let Err(error) = self.delegate.on_disconnected().await {
            let _ = self.delegate.on_error(error).await;
        }
    }
    async fn on_message(&self, message: TransportRes) {
        if let Err(error) = self.delegate.on_message(message).await {
            let _ = self.delegate.on_error(error).await;
        }
    }
}

impl<TransportRes> std::fmt::Debug for IrisListenerAdapter<TransportRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisListenerAdapter")
            .field("delegate", &"<Listener>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use iris::{CircuitBreakerConfig, ServerCloseBehavior};
    use serde::{Deserialize, Serialize};
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::Duration,
    };
    use tokio_stream::StreamExt;

    #[ctor::ctor]
    fn init() {
        ensure_crypto();
    }

    fn ensure_crypto() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    fn default_config() -> IrisConfig {
        IrisConfig::new().with_server_close_behavior(ServerCloseBehavior::Reconnect)
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct TestRequest {
        id: u64,
        method: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct TestResponse {
        id: u64,
        status: i32,
    }

    struct TestListener {
        received: Arc<Mutex<Vec<TestResponse>>>,
    }

    #[async_trait]
    impl ListenerTrait for TestListener {
        type TMessage = TestResponse;

        async fn on_message(&self, message: TestResponse) -> EGResult<()> {
            self.received
                .lock()
                .expect("mutex should not be poisoned")
                .push(message);
            Ok(())
        }
    }

    /// A listener whose `on_message` fails, recording the errors that are
    /// reported through `on_error`.
    struct FailingListener {
        errors: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ListenerTrait for FailingListener {
        type TMessage = TestResponse;

        async fn on_message(&self, _message: TestResponse) -> EGResult<()> {
            Err(EGError::BadResponse)
        }

        async fn on_error(&self, error: EGError) -> EGResult<()> {
            self.errors
                .lock()
                .expect("mutex should not be poisoned")
                .push(error.to_string());
            Ok(())
        }
    }

    /// A listener that signals when it enters `on_message` and then blocks
    /// forever, wedging the connection handler.
    struct BlockingListener {
        entered: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    }

    #[async_trait]
    impl ListenerTrait for BlockingListener {
        type TMessage = TestResponse;

        async fn on_message(&self, _message: TestResponse) -> EGResult<()> {
            let sender = self
                .entered
                .lock()
                .expect("mutex should not be poisoned")
                .take();
            if let Some(sender) = sender {
                let _ = sender.send(());
            }
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    /// Spawns a websocket server that replies to every message with a fixed
    /// [`TestResponse`] payload.
    async fn spawn_responder_server() -> (u16, tokio::sync::oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to ephemeral port");
        let port = listener
            .local_addr()
            .expect("should have local address")
            .port();
        tokio::spawn(async move {
            let mut shutdown_rx = Some(shutdown_rx);
            loop {
                tokio::select! {
                    _ = async {
                        if let Some(rx) = &mut shutdown_rx {
                            rx.await.ok();
                        }
                    } => break,
                    accept_result = listener.accept() => {
                        let (stream, _) = accept_result.expect("should accept a connection");
                        tokio::spawn(async move {
                            let builder = tokio_websockets::ServerBuilder::new();
                            if let Ok((_request, mut ws_stream)) = builder.accept(stream).await {
                                while let Some(Ok(message)) = ws_stream.next().await {
                                    if message.is_ping() {
                                        let payload = message.into_payload();
                                        let _ = ws_stream
                                            .send(tokio_websockets::Message::pong(payload))
                                            .await;
                                    } else if message.is_text() || message.is_binary() {
                                        let _ = ws_stream
                                            .send(tokio_websockets::Message::text(
                                                r#"{"id":1,"status":200}"#.to_string(),
                                            ))
                                            .await;
                                    } else if message.is_close() {
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });
        (port, shutdown_tx)
    }

    fn test_config() -> IrisConfig {
        default_config()
            .with_circuit_breaker_config(
                CircuitBreakerConfig::new()
                    .with_initial_backoff(Duration::from_millis(50))
                    .with_max_backoff(Duration::from_millis(200))
                    .with_max_reconnect_attempts(1),
            )
            .with_ping_interval(Duration::from_secs(3600))
            .with_pong_timeout(Duration::from_secs(3600))
    }

    /// Runs `body` with the runtime clock paused so it can drive timers with
    /// `advance` instead of sleeping. The clock is resumed before returning,
    /// so the rest of the test runs on real time.
    async fn with_paused_clock<T>(body: impl Future<Output = T>) -> T {
        tokio::time::pause();
        let result = body.await;
        tokio::time::resume();
        result
    }

    /// Advances the paused clock by `step` and yields so other tasks (real
    /// I/O handlers, reconnect timers) can make progress.
    async fn tick(step: Duration) {
        tokio::time::advance(step).await;
        tokio::task::yield_now().await;
    }

    async fn wait_until_connected(
        client: &dyn WebsocketClientTrait<TransportReq = TestRequest, TransportRes = TestResponse>,
    ) {
        with_paused_clock(async {
            for _ in 0..100 {
                if client.is_connected() {
                    return;
                }
                tick(Duration::from_millis(10)).await;
            }
            panic!("client did not connect within timeout");
        })
        .await;
    }

    /// A listener that counts how often the connection was established and
    /// records every received message.
    struct ConnectionCountingListener {
        connections: Arc<AtomicUsize>,
        received: Arc<Mutex<Vec<TestResponse>>>,
    }

    #[async_trait]
    impl ListenerTrait for ConnectionCountingListener {
        type TMessage = TestResponse;

        async fn on_connected(&self) -> EGResult<()> {
            self.connections.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn on_message(&self, message: TestResponse) -> EGResult<()> {
            self.received
                .lock()
                .expect("mutex should not be poisoned")
                .push(message);
            Ok(())
        }
    }

    /// Spawns a server that cleanly closes the first connection it accepts
    /// (as Binance does for maintenance or session expiry) and then behaves
    /// like the responder server for every later connection.
    async fn spawn_server_that_closes_first_connection() -> (u16, tokio::sync::oneshot::Sender<()>)
    {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to ephemeral port");
        let port = listener
            .local_addr()
            .expect("should have local address")
            .port();
        let connection_index = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            let mut shutdown_rx = Some(shutdown_rx);
            loop {
                tokio::select! {
                    _ = async {
                        if let Some(rx) = &mut shutdown_rx {
                            rx.await.ok();
                        }
                    } => break,
                    accept_result = listener.accept() => {
                        let (stream, _) = accept_result.expect("should accept a connection");
                        let index = connection_index.fetch_add(1, Ordering::SeqCst);
                        tokio::spawn(async move {
                            let builder = tokio_websockets::ServerBuilder::new();
                            if let Ok((_request, mut ws_stream)) = builder.accept(stream).await {
                                if index == 0 {
                                    // First connection: graceful server close.
                                    let _ = ws_stream
                                        .send(tokio_websockets::Message::close(
                                            Some(tokio_websockets::CloseCode::NORMAL_CLOSURE),
                                            "maintenance",
                                        ))
                                        .await;
                                    let _ = ws_stream.close().await;
                                } else {
                                    while let Some(Ok(message)) = ws_stream.next().await {
                                        if message.is_ping() {
                                            let payload = message.into_payload();
                                            let _ = ws_stream
                                                .send(tokio_websockets::Message::pong(payload))
                                                .await;
                                        } else if message.is_text() || message.is_binary() {
                                            let _ = ws_stream
                                                .send(tokio_websockets::Message::text(
                                                    r#"{"id":1,"status":200}"#.to_string(),
                                                ))
                                                .await;
                                        } else if message.is_close() {
                                            break;
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });
        (port, shutdown_tx)
    }

    /// Spawns a server that cleanly closes the first connection it accepts and
    /// then drops every later connection without completing the websocket
    /// handshake, so a reconnecting client's connect attempts keep failing and
    /// it stays in the reconnecting state (`is_connected` stays false) for the
    /// duration of the test.
    async fn spawn_server_that_closes_first_connection_then_stalls()
    -> (u16, tokio::sync::oneshot::Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to ephemeral port");
        let port = listener
            .local_addr()
            .expect("should have local address")
            .port();
        let connection_index = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            let mut shutdown_rx = Some(shutdown_rx);
            loop {
                tokio::select! {
                    _ = async {
                        if let Some(rx) = &mut shutdown_rx {
                            rx.await.ok();
                        }
                    } => break,
                    accept_result = listener.accept() => {
                        let (stream, _) = accept_result.expect("should accept a connection");
                        let index = connection_index.fetch_add(1, Ordering::SeqCst);
                        if index == 0 {
                            tokio::spawn(async move {
                                let builder = tokio_websockets::ServerBuilder::new();
                                if let Ok((_request, mut ws_stream)) = builder.accept(stream).await {
                                    // First connection: graceful server close, so the
                                    // client's `ServerCloseBehavior::Reconnect` kicks in.
                                    let _ = ws_stream
                                        .send(tokio_websockets::Message::close(
                                            Some(tokio_websockets::CloseCode::NORMAL_CLOSURE),
                                            "maintenance",
                                        ))
                                        .await;
                                    let _ = ws_stream.close().await;
                                }
                            });
                        } else {
                            // Later connections are dropped without a websocket
                            // upgrade, so the client's reconnect attempt fails and
                            // it keeps retrying, staying in the reconnecting state.
                            drop(stream);
                        }
                    }
                }
            }
        });
        (port, shutdown_tx)
    }

    /// Waits until `connections` has reached `expected`, driving the runtime
    /// clock forward with `pause`/`advance` instead of sleeping so the
    /// client's reconnect backoff timers fire without spending wall-clock
    /// time.
    async fn wait_until_connection_count(connections: &Arc<AtomicUsize>, expected: usize) {
        let result = with_paused_clock(async {
            for _ in 0..200 {
                if connections.load(Ordering::SeqCst) >= expected {
                    return Ok(());
                }
                // Fire any reconnect backoff timers due in the next window
                // and let the client's tasks run; the fresh websocket
                // handshake itself is real I/O and proceeds as usual.
                tick(Duration::from_millis(10)).await;
            }
            Err(connections.load(Ordering::SeqCst))
        })
        .await;
        if let Err(actual) = result {
            panic!("on_connected did not fire {expected} times, fired {actual} times");
        }
    }

    #[tokio::test]
    async fn reconnects_after_graceful_server_close() {
        let (port, _shutdown) = spawn_server_that_closes_first_connection().await;
        let connections = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener = Arc::new(ConnectionCountingListener {
            connections: connections.clone(),
            received: received.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let config = test_config().with_circuit_breaker_config(
            CircuitBreakerConfig::new()
                .with_initial_backoff(Duration::from_millis(50))
                .with_max_backoff(Duration::from_millis(200))
                .with_no_reconnect_limit(),
        );
        let client =
            IrisWebsocketClient::<TestRequest, TestResponse>::with_config(&url, config, listener);
        client.connect().await.expect("connect should succeed");
        wait_until_connection_count(&connections, 1).await;

        // The server closes the first connection cleanly. With
        // `ServerCloseBehavior::Reconnect` the client must come back and fire
        // `on_connected` again, keeping the reconnect/re-auth machinery alive.
        wait_until_connection_count(&connections, 2).await;
        assert!(
            client.is_connected(),
            "client should reconnect after server close"
        );

        // The fresh connection must be usable for round trips.
        let message = TestRequest {
            id: 1,
            method: "ping".into(),
        };
        client
            .send_message(message, Duration::from_secs(5))
            .await
            .expect("send on the fresh connection should succeed");
        let response = with_paused_clock(async {
            for _ in 0..100 {
                let response = {
                    let received = received.lock().expect("mutex should not be poisoned");
                    received.first().cloned()
                };
                if let Some(response) = response {
                    return response;
                }
                tick(Duration::from_millis(50)).await;
            }
            panic!("should receive a response on the fresh connection");
        })
        .await;
        assert_eq!(response, TestResponse { id: 1, status: 200 });
        client
            .disconnect()
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn send_message_fails_fast_while_reconnecting() {
        let (port, _shutdown) = spawn_server_that_closes_first_connection_then_stalls().await;
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener = Arc::new(TestListener {
            received: received.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let config = test_config().with_circuit_breaker_config(
            CircuitBreakerConfig::new()
                .with_initial_backoff(Duration::from_millis(50))
                .with_max_backoff(Duration::from_millis(200))
                .with_no_reconnect_limit(),
        );
        let client =
            IrisWebsocketClient::<TestRequest, TestResponse>::with_config(&url, config, listener);
        client.connect().await.expect("connect should succeed");
        wait_until_connected(&client).await;

        // The server closes the first connection cleanly, so with
        // `ServerCloseBehavior::Reconnect` the client starts reconnecting.
        // Every later handshake is stalled, so the client stays in the
        // reconnecting state and `is_connected` remains false.
        with_paused_clock(async {
            for _ in 0..200 {
                if !client.is_connected() {
                    return;
                }
                tick(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            !client.is_connected(),
            "client should drop into the reconnecting state after the server close"
        );

        // A message sent while the client is reconnecting must fail fast
        // instead of being buffered for the fresh connection: iris rejects
        // the send with `ConnectionClosed` as soon as the connected flag is
        // down, well inside the 30s gateway timeout. A buffering client would
        // accept the message and only deliver it after the reconnect, so a
        // prompt error proves the fail-fast behaviour.
        let message = TestRequest {
            id: 1,
            method: "ping".into(),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.send_message(message, Duration::from_secs(30)),
        )
        .await
        .expect("send while reconnecting should fail fast, not hang until its timeout");
        assert!(
            matches!(
                &result,
                Err(EGError::External(e))
                    if e
                        .downcast_ref::<iris::ConnectionError>()
                        .is_some_and(|error| {
                            matches!(error, iris::ConnectionError::ConnectionClosed)
                        })
            ),
            "send while reconnecting should fail fast with ConnectionClosed, got: {result:?}"
        );

        // The client is stuck in the reconnecting state with every handshake
        // stalled, so there is no live connection to close gracefully. iris's
        // graceful `disconnect` would wait out the full disconnect timeout
        // (10s by default) and then force-abort the connection task, which is
        // what made this test an order of magnitude slower than the others.
        // Abort the connection task right away instead.
        client
            .client
            .force_disconnect()
            .await
            .expect("force disconnect should succeed");
    }

    #[tokio::test]
    async fn on_message_error_is_reported_through_on_error() {
        let (port, _shutdown) = spawn_responder_server().await;
        let errors = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn ListenerTrait<TMessage = TestResponse>> = Arc::new(FailingListener {
            errors: errors.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let client = IrisWebsocketClient::<TestRequest, TestResponse>::with_config(
            &url,
            test_config(),
            listener,
        );
        client.connect().await.expect("connect should succeed");
        wait_until_connected(&client).await;
        let message = TestRequest {
            id: 1,
            method: "ping".into(),
        };
        client
            .send_message(message, Duration::from_secs(5))
            .await
            .expect("send should succeed");
        // The server replies and the listener's `on_message` fails: the
        // error must be sent through `on_error` instead of being silently
        // dropped by the adapter.
        let error = with_paused_clock(async {
            for _ in 0..100 {
                let error = {
                    let errors = errors.lock().expect("mutex should not be poisoned");
                    errors.first().cloned()
                };
                if let Some(error) = error {
                    return error;
                }
                tick(Duration::from_millis(50)).await;
            }
            panic!("on_error should be called with the message failure");
        })
        .await;
        assert_eq!(error, EGError::BadResponse.to_string());
        client
            .disconnect()
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn round_trip_send_and_receive() {
        let (port, _shutdown) = spawn_responder_server().await;
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn ListenerTrait<TMessage = TestResponse>> = Arc::new(TestListener {
            received: received.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let client = IrisWebsocketClient::<TestRequest, TestResponse>::with_config(
            &url,
            test_config(),
            listener,
        );
        client.connect().await.expect("connect should succeed");
        wait_until_connected(&client).await;
        let message = TestRequest {
            id: 1,
            method: "ping".into(),
        };
        client
            .send_message(message, Duration::from_secs(5))
            .await
            .expect("send should succeed");
        let response = with_paused_clock(async {
            for _ in 0..100 {
                let response = {
                    let received = received.lock().expect("mutex should not be poisoned");
                    received.first().cloned()
                };
                if let Some(response) = response {
                    return response;
                }
                tick(Duration::from_millis(50)).await;
            }
            panic!("should receive a response");
        })
        .await;
        assert_eq!(response, TestResponse { id: 1, status: 200 });
        client
            .disconnect()
            .await
            .expect("disconnect should succeed");
    }

    #[tokio::test]
    async fn send_message_when_not_connected_is_error() {
        let (port, _shutdown) = spawn_responder_server().await;
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn ListenerTrait<TMessage = TestResponse>> = Arc::new(TestListener {
            received: received.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let client = IrisWebsocketClient::<TestRequest, TestResponse>::with_config(
            &url,
            default_config(),
            listener,
        );
        let result = client
            .send_message(
                TestRequest {
                    id: 1,
                    method: "ping".into(),
                },
                Duration::from_secs(5),
            )
            .await;
        assert!(
            result.is_err(),
            "send before connect should fail, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn send_message_respects_timeout_when_handler_is_wedged() {
        let (port, _shutdown) = spawn_responder_server().await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let listener = Arc::new(BlockingListener {
            entered: Arc::new(Mutex::new(Some(entered_tx))),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let config = test_config().with_channel_buffer_size(1);
        let client =
            IrisWebsocketClient::<TestRequest, TestResponse>::with_config(&url, config, listener);
        client.connect().await.expect("connect should succeed");
        wait_until_connected(&client).await;

        let message = TestRequest {
            id: 1,
            method: "ping".into(),
        };
        // The server replies, the handler blocks inside the listener's
        // `on_message`, and can no longer drain the outgoing message channel.
        client
            .send_message(message.clone(), Duration::from_secs(1))
            .await
            .expect("trigger send should succeed");
        entered_rx.await.expect("handler should enter on_message");

        // The single-slot channel is now full and the handler is stuck, so a
        // further send can only ever complete by honoring its timeout.
        client
            .send_message(message.clone(), Duration::from_secs(1))
            .await
            .expect("message should be accepted into the channel");

        // Pause the clock and jump it past the timeout so the wedged send is
        // forced to time out without waiting real time. The production
        // `send_message` uses a real [`Delay`], so drive the timeout with a
        // tokio timer (fine in tests) that is registered against the paused
        // clock.
        let timeout = Duration::from_secs(1);
        tokio::time::pause();
        let send = client.send_message_with_delay(message, tokio::time::sleep(timeout));
        tokio::pin!(send);
        // Poll once so the internal timeout timer is registered against the
        // paused clock; the wedged send itself remains pending.
        assert!(
            futures::poll!(send.as_mut()).is_pending(),
            "send on a wedged connection should be pending"
        );
        // Advance the clock past the timeout in a single jump: the timeout
        // fires immediately and the send completes without real time passing.
        tokio::time::advance(timeout + Duration::from_millis(1)).await;
        let result = match futures::poll!(send.as_mut()) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("send should complete once the timeout fires"),
        };
        tokio::time::resume();

        assert!(
            matches!(result, Err(EGError::TimedOut)),
            "send on a wedged connection should time out with TimedOut, got: {result:?}"
        );
    }
}
