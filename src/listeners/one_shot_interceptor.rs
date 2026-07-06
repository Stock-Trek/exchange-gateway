use crate::{
    error::{EGError, EGResult},
    functions::FilterMessage,
};
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    time::Duration,
};

pub(crate) type OneShotInterceptor<TResponse> = Arc<dyn OneShotInterceptorTrait<TResponse>>;

pub(crate) trait OneShotInterceptorTrait<TResponse>: Send + Sync {
    fn intercept(&self, response: &TResponse) -> bool;
}

pub(crate) struct OneShotInterceptorImpl<TResponse, TIntercepted> {
    filter_response: FilterMessage<TResponse, TIntercepted>,
    sender_lock: Mutex<Option<Sender<TIntercepted>>>,
    receiver_lock: Mutex<Option<Receiver<TIntercepted>>>,
}

impl<TResponse, TIntercepted> OneShotInterceptorImpl<TResponse, TIntercepted> {
    pub fn new(filter_response: FilterMessage<TResponse, TIntercepted>) -> Self {
        let (sender, receiver) = channel();
        Self {
            filter_response,
            sender_lock: Mutex::new(Some(sender)),
            receiver_lock: Mutex::new(Some(receiver)),
        }
    }
    pub fn wait(&self, timeout: Duration) -> EGResult<TIntercepted> {
        let receiver = self
            .receiver_lock
            .lock()
            .map_err(|_| EGError::Poison)?
            .take()
            .ok_or(EGError::OneShotAlreadyUsed)?;
        match receiver.recv_timeout(timeout) {
            Ok(intercepted) => Ok(intercepted),
            Err(e) => Err(EGError::ReceiveTimeout(e)),
        }
    }
}

impl<TResponse, TIntercepted> OneShotInterceptorTrait<TResponse>
    for OneShotInterceptorImpl<TResponse, TIntercepted>
where
    TResponse: Send + Sync,
    TIntercepted: Send,
{
    fn intercept(&self, response: &TResponse) -> bool {
        match (self.filter_response)(response) {
            Some(intercepted) => {
                if let Ok(mut guard) = self.sender_lock.lock() {
                    if let Some(sender) = guard.take() {
                        drop(guard);
                        match sender.send(intercepted) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("Error when sending intercepted response: {}", e);
                            }
                        }
                    } else {
                        eprintln!("Sender already used or not available");
                    }
                } else {
                    eprintln!("Failed to lock sender");
                }
                true
            }
            None => false,
        }
    }
}
