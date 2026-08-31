use crate::error::{EGError, EGResult};
use async_trait::async_trait;

#[async_trait]
pub trait ListenerTrait: Send + Sync {
    type TMessage;

    async fn on_connected(&self) -> EGResult<()> {
        Ok(())
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        Ok(())
    }
    /// Reports an error that occurred while processing the stream, for
    /// example a message that failed to convert into `Self::TMessage`.
    async fn on_error(&self, _error: &EGError) -> EGResult<()> {
        Ok(())
    }
    async fn on_message(&self, message: Self::TMessage) -> EGResult<()>;
}
