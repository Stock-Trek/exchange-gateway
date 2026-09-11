use crate::{
    clients::client::WebsocketClient,
    error::{EGError, EGResult, SendFailure, SendResult},
    panic_guard::PanicUtils,
    websocket_listener::WebsocketListener,
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
    pub fn with_config(url: &str, config: IrisConfig, listener: Arc<WebsocketListener>) -> Self {
        let client = IrisClient::new(
            config,
            Arc::new(IrisListenerAdapter { delegate: listener }),
            url,
        );
        Self { client }
    }
    async fn send_message_with_delay<D>(&self, message: String, delay: D) -> SendResult<()>
    where
        D: Future<Output = ()> + Send + 'static,
    {
        let mut send = Box::pin(self.client.send(message));
        let mut delay = Box::pin(delay);
        poll_fn(move |cx| match send.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(Self::map_send_error)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(SendFailure::not_sent(EGError::TimedOut))),
                Poll::Pending => Poll::Pending,
            },
        })
        .await
    }
    fn map_send_error(error: ConnectionError) -> SendFailure {
        match error {
            ConnectionError::ConnectionClosed | ConnectionError::SendMessage(_) => {
                SendFailure::not_sent(EGError::External(Box::new(error)))
            }
            error => SendFailure::unknown(EGError::External(Box::new(error))),
        }
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
    async fn send(&self, message: String, timeout: Duration) -> SendResult<()> {
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

struct IrisListenerAdapter {
    delegate: Arc<WebsocketListener>,
}

#[async_trait]
impl IrisListener<serde_json::Value> for IrisListenerAdapter {
    async fn on_message(&self, message: serde_json::Value) {
        let future = self.delegate.on_message(message);
        let _ = match PanicUtils::catch_panic_async(future).await {
            Ok(result) => result,
            Err(payload) => {
                let error = EGError::CallbackPanicked(PanicUtils::panic_message(payload.as_ref()));
                eprintln!("Error when calling `on_message`: {}", error);
                Ok(())
            }
        };
    }
}

impl std::fmt::Debug for IrisListenerAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisListenerAdapter")
            .field("delegate", &"<Listener>")
            .finish()
    }
}
