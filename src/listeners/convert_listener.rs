use crate::{
    error::EGResult,
    functions::ArcTryConvertValue,
    listeners::listener::{Listener, ListenerTrait},
};
use async_trait::async_trait;

pub(crate) struct ConvertListener<TFrom, TTo> {
    converter: ArcTryConvertValue<TFrom, TTo>,
    delegate: Listener<TTo>,
}

impl<TFrom, TTo> ConvertListener<TFrom, TTo> {
    pub fn new(converter: ArcTryConvertValue<TFrom, TTo>, delegate: Listener<TTo>) -> Self {
        Self {
            converter,
            delegate,
        }
    }
}

#[async_trait]
impl<TFrom, TTo> ListenerTrait<TFrom> for ConvertListener<TFrom, TTo>
where
    TFrom: Send,
    TTo: Send,
{
    async fn on_message(&self, message: TFrom) -> EGResult<()> {
        let converted = (self.converter)(message)?;
        self.delegate.on_message(converted).await
    }
}
