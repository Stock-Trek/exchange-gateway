use crate::{
    error::{EGError, EGResult},
    functions::TryConvertResponseFrom,
    listeners::listener::{Listener, ListenerTrait},
};
use async_trait::async_trait;
use std::sync::RwLock;

pub(crate) struct ConvertListener<TFrom, TTo> {
    converter: TryConvertResponseFrom<TFrom, TTo>,
    delegate_lock: RwLock<Listener<TTo>>,
}

impl<TFrom, TTo> ConvertListener<TFrom, TTo> {
    pub fn new(converter: TryConvertResponseFrom<TFrom, TTo>, delegate: Listener<TTo>) -> Self {
        Self {
            converter,
            delegate_lock: RwLock::new(delegate),
        }
    }
    pub fn set_delegate(&self, delegate: Listener<TTo>) -> EGResult<()> {
        let mut guard = self.delegate_lock.write().map_err(|_| EGError::Poison)?;
        *guard = delegate;
        Ok(())
    }
}

#[async_trait]
impl<TFrom, TTo> ListenerTrait<TFrom> for ConvertListener<TFrom, TTo>
where
    TFrom: Send,
{
    async fn on_message(&self, message: TFrom) -> EGResult<()> {
        let converted = (self.converter)(message)?;
        let delegate = self
            .delegate_lock
            .read()
            .map_err(|_| EGError::Poison)?
            .clone();
        delegate.on_message(converted).await
    }
}
