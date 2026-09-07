use crate::{
    clients::client::WebsocketClient,
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;
use futures_timer::Delay;
use iris::{Client as IrisClient, Config as IrisConfig, ConnectionError, Listener as IrisListener};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

pub struct IrisWebsocketClient {
    client: IrisClient<String, serde_json::Value>,
}

impl IrisWebsocketClient {
    pub fn with_config(
        url: &str,
        config: IrisConfig,
        listener: Arc<dyn ListenerTrait<TMessage = serde_json::Value>>,
    ) -> Self {
        let client = IrisClient::new(
            config,
            Arc::new(IrisListenerAdapter { delegate: listener }),
            url,
        );
        Self { client }
    }
    async fn send_message_with_delay<D>(&self, message: String, delay: D) -> EGResult<()>
    where
        D: Future<Output = ()> + Send + 'static,
    {
        let mut send = Box::pin(self.client.send(message));
        let mut delay = Box::pin(delay);
        poll_fn(move |cx| match send.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(map_send_error)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
        .await
    }
}

#[async_trait]
impl WebsocketClient for IrisWebsocketClient {
    async fn connect(&self) -> EGResult<()> {
        self.client
            .connect()
            .await
            .map_err(|e| EGError::External(Box::new(e)))
    }
    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }
    async fn send(&self, message: String, timeout: Duration) -> EGResult<()> {
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

impl std::fmt::Debug for IrisWebsocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisWebsocketClient")
            .field("client", &"<websocket::WebsocketClient>")
            .finish()
    }
}

/// Maps an error from `IrisClient::send` into an `EGError`.
///
/// iris reports a send failure only before the message is enqueued onto its
/// outbound channel: it fails fast with [`ConnectionError::ConnectionClosed`]
/// whenever the client is not connected (never connected, disconnected, or
/// down while reconnecting), and returns [`ConnectionError::SendMessage`] when
/// the channel closes mid-send. In both cases the message is never transmitted
/// to the exchange, so surface them as [`EGError::NotSent`] to let
/// `Connector::on_error` refund the rate-limit tokens acquired for the
/// request — mirroring how the HTTP client maps connect failures to
/// `NotSent`. Any other error (e.g. poisoned internal state) keeps the
/// conservative `External` classification, which is not refunded.
fn map_send_error(error: ConnectionError) -> EGError {
    match error {
        ConnectionError::ConnectionClosed | ConnectionError::SendMessage(_) => {
            EGError::NotSent(Box::new(EGError::External(Box::new(error))))
        }
        error => EGError::External(Box::new(error)),
    }
}

struct IrisListenerAdapter {
    delegate: Arc<dyn ListenerTrait<TMessage = serde_json::Value>>,
}

#[async_trait]
impl IrisListener<serde_json::Value> for IrisListenerAdapter {
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
    async fn on_message(&self, message: serde_json::Value) {
        if let Err(error) = self.delegate.on_message(message).await {
            let _ = self.delegate.on_error(error).await;
        }
    }
}

impl std::fmt::Debug for IrisListenerAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisListenerAdapter")
            .field("delegate", &"<Listener>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_wire_send_failures_are_mapped_to_not_sent() {
        // Send failures that happen before the message is enqueued onto the
        // outbound channel (fail-fast while disconnected, or the channel
        // closing mid-send) must surface as NotSent so the connector refunds
        // the rate-limit tokens; anything else stays External (no refund).
        let error = map_send_error(ConnectionError::ConnectionClosed);
        assert!(matches!(error, EGError::NotSent(_)));

        let error = map_send_error(ConnectionError::SendMessage("closed".into()));
        assert!(matches!(error, EGError::NotSent(_)));

        let error = map_send_error(ConnectionError::InternalState);
        assert!(matches!(error, EGError::External(_)));
    }
}
