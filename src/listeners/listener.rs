use crate::error::EGResult;
use async_trait::async_trait;

pub type Listener<TMessage> = Box<dyn ListenerTrait<TMessage>>;

#[async_trait]
pub trait ListenerTrait<TMessage>: Send + Sync {
    async fn on_message(&self, message: TMessage) -> EGResult<()>;
}
