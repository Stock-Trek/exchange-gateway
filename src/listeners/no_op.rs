use crate::{error::EGResult, listeners::listener::ListenerTrait};
use async_trait::async_trait;
use std::marker::PhantomData;

pub(crate) struct NoOpListener<TMessage> {
    _phantom: PhantomData<TMessage>,
}

impl<TMessage> NoOpListener<TMessage> {
    pub(crate) fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<TMessage> ListenerTrait for NoOpListener<TMessage>
where
    TMessage: Send + Sync,
{
    type TMessage = TMessage;

    async fn on_message(&self, _message: TMessage) -> EGResult<()> {
        Ok(())
    }
}
