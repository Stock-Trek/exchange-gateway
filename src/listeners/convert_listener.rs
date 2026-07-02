use crate::{
    error::EGResult,
    functions::TryConvertResponseFrom,
    listeners::listener::{Listener, ListenerTrait},
};
use async_trait::async_trait;

pub struct ConvertListener<TFrom, TTo> {
    converter: TryConvertResponseFrom<TFrom, TTo>,
    listener: Listener<TTo>,
}

impl<TFrom, TTo> ConvertListener<TFrom, TTo> {
    pub fn new(converter: TryConvertResponseFrom<TFrom, TTo>, listener: Listener<TTo>) -> Self {
        Self {
            converter,
            listener,
        }
    }
}

#[async_trait]
impl<TFrom, TTo> ListenerTrait<TFrom> for ConvertListener<TFrom, TTo>
where
    TFrom: Send,
{
    async fn on_message(&self, message: TFrom) -> EGResult<()> {
        let converted = (self.converter)(message)?;
        self.listener.on_message(converted).await
    }
}
