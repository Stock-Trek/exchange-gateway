use crate::error::EGResult;
use async_trait::async_trait;

#[async_trait]
pub trait ListenerTrait: Send + Sync {
    type TMessage;

    async fn on_message(&self, message: Self::TMessage) -> EGResult<()>;

    /// Called when the underlying connection has been established, including
    /// after an automatic reconnect.
    async fn on_connected(&self) -> EGResult<()> {
        Ok(())
    }
}
