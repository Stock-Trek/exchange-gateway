use crate::{
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

pub struct OneShotListener<T: Send> {
    state: Arc<Mutex<State<T>>>,
}

impl<T: Send> OneShotListener<T> {
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: FnOnce(T) -> Fut + Send + 'static,
        Fut: Future<Output = EGResult<()>> + Send + 'static,
    {
        let handler = Box::new(move |msg: T| {
            Box::pin(f(msg)) as Pin<Box<dyn Future<Output = EGResult<()>> + Send>>
        });
        OneShotListener {
            state: Arc::new(Mutex::new(State {
                message: None,
                handler: Some(handler),
                waiter_set: false,
                waker: None,
            })),
        }
    }
    pub async fn wait_for_message(&self) -> EGResult<T> {
        {
            let mut guard = self.state.lock().unwrap();
            if guard.waiter_set {
                return Err(EGError::OneShotCalledTwice);
            }
            if let Some(msg) = guard.message.take() {
                return Ok(msg);
            }
            guard.waiter_set = true;
        }
        WaitFuture {
            state: Arc::clone(&self.state),
        }
        .await
    }
}

#[async_trait]
impl<T: Send> ListenerTrait<T> for OneShotListener<T> {
    async fn on_message(&self, message: T) -> EGResult<()> {
        let handler = {
            let mut guard = self.state.lock().unwrap();
            if guard.waiter_set {
                guard.message = Some(message);
                if let Some(waker) = guard.waker.take() {
                    waker.wake();
                }
                return Ok(());
            }
            guard.handler.take()
        };
        match handler {
            Some(f) => f(message).await,
            None => Ok(()),
        }
    }
}

struct State<T: Send> {
    message: Option<T>,
    handler:
        Option<Box<dyn FnOnce(T) -> Pin<Box<dyn Future<Output = EGResult<()>> + Send>> + Send>>,
    waiter_set: bool,
    waker: Option<Waker>,
}

struct WaitFuture<T: Send> {
    state: Arc<Mutex<State<T>>>,
}

impl<T: Send> Future for WaitFuture<T> {
    type Output = EGResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self.state.lock().unwrap();
        if let Some(msg) = guard.message.take() {
            Poll::Ready(Ok(msg))
        } else {
            guard.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
