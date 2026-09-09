use crate::{error::EGResult, listeners::listener::ListenerTrait};
use async_trait::async_trait;

pub(crate) struct NoOpListener;

#[async_trait]
impl ListenerTrait for NoOpListener {
    type TMessage = serde_json::Value;

    async fn on_message(&self, _message: serde_json::Value) -> EGResult<()> {
        Ok(())
    }
}
