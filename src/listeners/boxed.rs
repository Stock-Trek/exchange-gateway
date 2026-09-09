use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;

pub(crate) struct BoxedListener<Message>(pub(crate) Box<dyn ListenerTrait<TMessage = Message>>);

#[async_trait]
impl<TMessage> ListenerTrait for BoxedListener<TMessage>
where
    TMessage: Send,
{
    type TMessage = TMessage;

    async fn on_connected(&self) -> EGResult<()> {
        self.0.on_connected().await
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        self.0.on_disconnected().await
    }
    async fn on_error(&self, error: EGError) -> EGResult<()> {
        self.0.on_error(error).await
    }
    async fn on_message(&self, message: TMessage) -> EGResult<()> {
        self.0.on_message(message).await
    }
}
