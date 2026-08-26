//! A concrete [`WebsocketClientTrait`] implementation backed by the
//! [`iris`] crate.
use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
    transports::websocket::WebsocketClientTrait,
};
use async_trait::async_trait;
use futures_timer::Delay;
use iris::{Client as IrisClient, Config as IrisConfig, Listener as IrisListener};
use serde::{Serialize, de::DeserializeOwned};
use std::{future::poll_fn, sync::Arc, task::Poll, time::Duration};

/// A concrete [`WebsocketClientTrait`] implementation backed by the
/// [`iris`] crate.
///
/// Outgoing [`TransportReq`](WebsocketClientTrait::TransportReq) messages are
/// serialized to JSON before being sent over the wire and incoming frames are
/// deserialized into [`TransportRes`](WebsocketClientTrait::TransportRes) messages,
/// which are forwarded to the listener supplied at construction time.
pub struct IrisWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + 'static,
    TransportRes: DeserializeOwned + Send + 'static,
{
    client: IrisClient<TransportReq, TransportRes>,
    listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
}

impl<TransportReq, TransportRes> IrisWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + 'static,
    TransportRes: DeserializeOwned + Send + 'static,
{
    /// Creates a client that connects to `url` using a default
    /// [`IrisConfig`].
    pub fn new(url: &str, listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>) -> Self {
        Self::with_config(url, IrisConfig::new(), listener)
    }

    /// Creates a client that connects to `url` using a custom
    /// [`IrisConfig`].
    pub fn with_config(
        url: &str,
        config: IrisConfig,
        listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
    ) -> Self {
        let client = IrisClient::new(
            config,
            Arc::new(IrisListenerAdapter {
                delegate: listener.clone(),
            }),
            url,
        );
        Self { client, listener }
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
        let mut send = Box::pin(self.client.send(message));
        let mut delay = Box::pin(Delay::new(timeout));
        poll_fn(move |cx| match send.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(|e| EGError::External(Box::new(e)))),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
        .await
    }

    async fn on_message(&self, message: Self::TransportRes) -> EGResult<()> {
        self.listener.on_message(message).await
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
            .field("listener", &"<Listener>")
            .finish()
    }
}

/// Adapts an [`Arc<dyn ListenerTrait>`] to the [`IrisListener`] expected by the [`iris`] crate
struct IrisListenerAdapter<TransportRes> {
    delegate: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
}

#[async_trait]
impl<TransportRes> IrisListener<TransportRes> for IrisListenerAdapter<TransportRes>
where
    TransportRes: DeserializeOwned + Send + 'static,
{
    async fn on_message(&self, message: TransportRes) {
        let _ = self.delegate.on_message(message).await;
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
    use iris::CircuitBreakerConfig;
    use serde::{Deserialize, Serialize};
    use std::{
        sync::{Arc, Mutex},
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
        IrisConfig::new()
            .with_circuit_breaker_config(
                CircuitBreakerConfig::new()
                    .with_initial_backoff(Duration::from_millis(50))
                    .with_max_backoff(Duration::from_millis(200))
                    .with_max_reconnect_attempts(1),
            )
            .with_ping_interval(Duration::from_secs(3600))
            .with_pong_timeout(Duration::from_secs(3600))
    }

    async fn wait_until_connected(
        client: &dyn WebsocketClientTrait<TransportReq = TestRequest, TransportRes = TestResponse>,
    ) {
        for _ in 0..100 {
            if client.is_connected() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("client did not connect within timeout");
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
        let response = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let response = {
                    let received = received.lock().expect("mutex should not be poisoned");
                    received.first().cloned()
                };
                if let Some(response) = response {
                    return response;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("should receive a response");
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
        let client = IrisWebsocketClient::<TestRequest, TestResponse>::new(&url, listener);
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
        let listener: Arc<dyn ListenerTrait<TMessage = TestResponse>> =
            Arc::new(BlockingListener {
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

        // The single-slot channel is now full and the handler is stuck, so
        // this send can only ever complete by honoring the timeout. The
        // timeout is deliberately small so the test finishes quickly.
        client
            .send_message(message.clone(), Duration::from_secs(1))
            .await
            .expect("message should be accepted into the channel");

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            client.send_message(message, Duration::from_millis(100)),
        )
        .await
        .expect("send_message should return within the timeout");
        assert!(
            matches!(result, Err(EGError::TimedOut)),
            "send on a wedged connection should time out with TimedOut, got: {result:?}"
        );
    }
}
