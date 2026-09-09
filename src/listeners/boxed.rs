use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;

pub(crate) struct BoxedListener(pub(crate) Box<dyn ListenerTrait<TMessage = serde_json::Value>>);

#[async_trait]
impl ListenerTrait for BoxedListener {
    type TMessage = serde_json::Value;

    async fn on_connected(&self) -> EGResult<()> {
        self.0.on_connected().await
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        self.0.on_disconnected().await
    }
    async fn on_error(&self, error: EGError) -> EGResult<()> {
        self.0.on_error(error).await
    }
    async fn on_message(&self, message: serde_json::Value) -> EGResult<()> {
        self.0.on_message(message).await
    }
}
