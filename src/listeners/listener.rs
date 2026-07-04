use crate::error::EGResult;
use async_trait::async_trait;
use std::sync::Arc;

pub type Listener<TMessage> = Arc<dyn ListenerTrait<TMessage>>;

#[async_trait]
pub trait ListenerTrait<TMessage>: Send + Sync {
    async fn on_message(&self, message: TMessage) -> EGResult<()>;
}
