use crate::{
    error::{EGError, EGResult},
    functions::FilterMessage,
};
use async_trait::async_trait;
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::sync::{
    Mutex,
    oneshot::{Receiver, Sender, channel},
};

pub(crate) type Interceptor<TResponse> = Arc<dyn InterceptorTrait<TResponse>>;

#[async_trait]
pub(crate) trait InterceptorTrait<TResponse>: Send + Sync {
    async fn intercept(&self, response: &TResponse) -> bool;
}

pub(crate) struct InterceptorImpl<TResponse, TIntercepted> {
    converter: FilterMessage<TResponse, TIntercepted>,
    sender_lock: Mutex<Option<Sender<TIntercepted>>>,
    receiver: Receiver<TIntercepted>,
}

impl<TResponse, TIntercepted> InterceptorImpl<TResponse, TIntercepted> {
    pub fn new(converter: FilterMessage<TResponse, TIntercepted>) -> Self {
        let (sender, receiver) = channel();
        Self {
            converter,
            sender_lock: Mutex::new(Some(sender)),
            receiver,
        }
    }
    pub async fn wait(self, timeout: Duration) -> EGResult<TIntercepted> {
        match tokio::time::timeout(timeout, self.receiver).await {
            Ok(Ok(intercepted)) => Ok(intercepted),
            Ok(Err(e)) => Err(EGError::ReceiveError(e)),
            Err(_) => Err(EGError::Timeout(timeout)),
        }
    }
}

#[async_trait]
impl<TResponse, TIntercepted> InterceptorTrait<TResponse>
    for InterceptorImpl<TResponse, TIntercepted>
where
    TResponse: Send + Sync,
    TIntercepted: Display + Send,
{
    async fn intercept(&self, response: &TResponse) -> bool {
        match (self.converter)(response) {
            Some(intercepted) => {
                let mut guard = self.sender_lock.lock().await;
                if let Some(sender) = guard.take() {
                    drop(guard);
                    sender.send(intercepted);
                } else {
                    eprintln!("Sender already used or not available");
                }
                true
            }
            None => false,
        }
    }
}
