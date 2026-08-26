use crate::error::EGResult;
use async_trait::async_trait;

#[async_trait]
pub trait ListenerTrait: Send + Sync {
    type TMessage;

    async fn on_connected(&self) -> EGResult<()> {
        Ok(())
    }
    async fn on_message(&self, message: Self::TMessage) -> EGResult<()>;
}
