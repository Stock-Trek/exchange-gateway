use crate::{
    error::{EGError, EGResult},
    functions::TryConvertResponseFrom,
    listeners::{
        interceptor::Interceptor,
        listener::{Listener, ListenerTrait},
    },
};
use async_trait::async_trait;
use std::sync::{Arc, RwLock};

pub(crate) struct ExchangeListener<TFrom, TTo> {
    converter: TryConvertResponseFrom<TFrom, TTo>,
    interceptors: Arc<RwLock<Vec<Interceptor<TTo>>>>,
    delegate: Listener<TTo>,
}

impl<TFrom, TTo> ExchangeListener<TFrom, TTo> {
    pub fn new(converter: TryConvertResponseFrom<TFrom, TTo>, delegate: Listener<TTo>) -> Self {
        Self {
            converter,
            interceptors: Arc::new(RwLock::new(Vec::new())),
            delegate,
        }
    }
    pub fn add_interceptor(&self, interceptor: Interceptor<TTo>) -> EGResult<()> {
        let mut guard = self.interceptors.write().map_err(|_| EGError::Poison)?;
        (*guard).push(interceptor);
        Ok(())
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
        let interceptors = {
            let guard = self.interceptors.read().map_err(|_| EGError::Poison)?;
            guard.iter().map(|arc| arc.clone()).collect::<Vec<_>>()
        };
        let mut accepted_interceptor = None;
        for interceptor in interceptors {
            if interceptor.intercept(&converted).await {
                accepted_interceptor = Some(interceptor);
                break;
            }
        }
        if let Some(accepted) = accepted_interceptor {
            let mut guard = self.interceptors.write().map_err(|_| EGError::Poison)?;
            guard.retain(|arc| !Arc::ptr_eq(arc, &accepted));
            return Ok(());
        }
        self.delegate.on_message(converted).await
    }
}
