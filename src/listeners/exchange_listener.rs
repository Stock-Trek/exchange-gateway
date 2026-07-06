use crate::{
    error::EGResult,
    functions::TryConvertResponseFrom,
    listeners::listener::{Listener, ListenerTrait},
};
use async_trait::async_trait;

pub(crate) struct ExchangeListener<TFrom, TTo> {
    converter: TryConvertResponseFrom<TFrom, TTo>,
    delegate: Listener<TTo>,
}

impl<TFrom, TTo> ExchangeListener<TFrom, TTo> {
    pub fn new(converter: TryConvertResponseFrom<TFrom, TTo>, delegate: Listener<TTo>) -> Self {
        Self {
            converter,
            delegate,
        }
    }
}

#[async_trait]
impl<TFrom, TTo> ListenerTrait<TFrom> for ExchangeListener<TFrom, TTo>
where
    TFrom: Send,
    TTo: Send,
{
    async fn on_message(&self, message: TFrom) -> EGResult<()> {
        let converted = (self.converter)(message)?;
        self.delegate.on_message(converted).await
    }
}
