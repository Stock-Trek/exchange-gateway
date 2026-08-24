//! A concrete [`WebsocketClientTrait`] implementation backed by the shipyard
//! [`websocket`] crate.

use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
    transports::websocket::WebsocketClientTrait,
};
use ::websocket::prelude::{MessageListenerTrait, WebsocketClient, WebsocketConfig};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc, time::Duration};

/// A concrete [`WebsocketClientTrait`] implementation backed by the shipyard
/// [`websocket`] crate.
///
/// Outgoing [`TransportReq`](WebsocketClientTrait::TransportReq) messages are
/// serialized to JSON before being sent over the wire and incoming websocket
/// frames are deserialized into
/// [`TransportRes`](WebsocketClientTrait::TransportRes) messages, which are
/// forwarded to the listener supplied at construction time.
pub struct ShipyardWebsocketClient<TransportReq, TransportRes> {
    client: WebsocketClient<serde_json::Value>,
    listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
    _transport_req: PhantomData<TransportReq>,
}

impl<TransportReq, TransportRes> ShipyardWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + Sync + 'static,
    TransportRes: DeserializeOwned + Send + Sync + 'static,
{
    /// Creates a client that connects to `url` using a default
    /// [`WebsocketConfig`].
    pub fn new(url: &str, listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>) -> Self {
        Self::with_config(url, WebsocketConfig::new(), listener)
    }

    /// Creates a client that connects to `url` using a custom
    /// [`WebsocketConfig`].
    pub fn with_config(
        url: &str,
        config: WebsocketConfig,
        listener: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
    ) -> Self {
        let client = WebsocketClient::new(
            config,
            Arc::new(ValueMessageListener {
                delegate: listener.clone(),
            }),
            url,
        );
        Self {
            client,
            listener,
            _transport_req: PhantomData,
        }
    }
}

#[async_trait]
impl<TransportReq, TransportRes> WebsocketClientTrait
    for ShipyardWebsocketClient<TransportReq, TransportRes>
where
    TransportReq: Serialize + Send + Sync + 'static,
    TransportRes: DeserializeOwned + Send + Sync + 'static,
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

    async fn send_message(&self, message: Self::TransportReq, _timeout: Duration) -> EGResult<()> {
        let message = serde_json::to_value(&message).map_err(|e| EGError::External(Box::new(e)))?;
        self.client
            .send(message)
            .await
            .map_err(|e| EGError::External(Box::new(e)))
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

impl<TransportReq, TransportRes> std::fmt::Debug
    for ShipyardWebsocketClient<TransportReq, TransportRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShipyardWebsocketClient")
            .field("client", &"<websocket::WebsocketClient>")
            .field("listener", &"<Listener>")
            .finish()
    }
}

/// Adapts an [`Arc<dyn ListenerTrait>`] to the [`MessageListenerTrait`]
/// expected by the shipyard [`websocket`] crate by deserializing each
/// incoming JSON value into the transport response type.
struct ValueMessageListener<TransportRes> {
    delegate: Arc<dyn ListenerTrait<TMessage = TransportRes>>,
}

#[async_trait]
impl<TransportRes> MessageListenerTrait<serde_json::Value> for ValueMessageListener<TransportRes>
where
    TransportRes: DeserializeOwned + Send + 'static,
{
    async fn on_message(&self, message: serde_json::Value) {
        match serde_json::from_value::<TransportRes>(message) {
            Ok(message) => {
                // The websocket crate listener interface cannot propagate
                // errors, so delivery failures are logged and dropped.
                let _ = self.delegate.on_message(message).await;
            }
            Err(error) => {
                eprintln!(
                    "websocket message could not be deserialized into transport response: {error}"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::websocket::prelude::CircuitBreakerConfig;
    use futures::SinkExt;
    use serde::{Deserialize, Serialize};
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio_stream::StreamExt;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestRequest {
        id: u64,
        method: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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

    fn test_config() -> WebsocketConfig {
        WebsocketConfig::new()
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
        for _ in 0..30 {
            if client.is_connected() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("client did not connect within timeout");
    }

    #[tokio::test]
    async fn round_trip_send_and_receive() {
        ::websocket::init_tests::ensure_crypto();
        let (port, _shutdown) = spawn_responder_server().await;
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn ListenerTrait<TMessage = TestResponse>> = Arc::new(TestListener {
            received: received.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let client = ShipyardWebsocketClient::<TestRequest, TestResponse>::with_config(
            &url,
            test_config(),
            listener,
        );
        client.connect().await.expect("connect should succeed");
        wait_until_connected(&client).await;
        client
            .send_message(
                TestRequest {
                    id: 1,
                    method: "ping".into(),
                },
                Duration::from_secs(5),
            )
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
        ::websocket::init_tests::ensure_crypto();
        let (port, _shutdown) = spawn_responder_server().await;
        let received = Arc::new(Mutex::new(Vec::new()));
        let listener: Arc<dyn ListenerTrait<TMessage = TestResponse>> = Arc::new(TestListener {
            received: received.clone(),
        });
        let url = format!("ws://127.0.0.1:{port}/ws");
        let client = ShipyardWebsocketClient::<TestRequest, TestResponse>::new(&url, listener);
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
}
