use crate::{error::EGResult, listeners::listener::ListenerTrait};
use async_trait::async_trait;
use std::marker::PhantomData;

pub(crate) struct IgnoreListener<T> {
    _phantom: PhantomData<T>,
}

impl<T> IgnoreListener<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> ListenerTrait<T> for IgnoreListener<T>
where
    T: Send + Sync,
{
    async fn on_message(&self, _message: T) -> EGResult<()> {
        Ok(())
    }
}
