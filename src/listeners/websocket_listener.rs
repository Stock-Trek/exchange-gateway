use crate::{
    error::EGResult,
    functions::TryConvertResponseFrom,
    listeners::listener::{Listener, ListenerTrait},
    transports::websocket::WebsocketMessageDto,
};
use async_trait::async_trait;
use std::{
    future::poll_fn,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

struct WaiterState<TResponse: Send> {
    message: Option<TResponse>,
    waker: Option<Waker>,
}

struct WaitEntry<TResponse: Send> {
    filter: Box<dyn Fn(&TResponse) -> bool + Send>,
    state: Arc<Mutex<WaiterState<TResponse>>>,
}

pub struct WebsocketListener<TResponse: Send> {
    converter: TryConvertResponseFrom<WebsocketMessageDto, TResponse>,
    delegate: Listener<TResponse>,
    state: Arc<Mutex<Vec<WaitEntry<TResponse>>>>,
}

impl<TResponse: Send> WebsocketListener<TResponse> {
    pub fn new(
        converter: TryConvertResponseFrom<WebsocketMessageDto, TResponse>,
        delegate: Listener<TResponse>,
    ) -> Self {
        WebsocketListener {
            converter,
            delegate,
            state: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn wait_for_message<F>(&self, filter: F) -> EGResult<TResponse>
    where
        F: Fn(&TResponse) -> bool + Send + 'static,
    {
        let waiter_state = Arc::new(Mutex::new(WaiterState {
            message: None,
            waker: None,
        }));
        let entry = WaitEntry {
            filter: Box::new(filter),
            state: waiter_state.clone(),
        };
        {
            let mut guard = self.state.lock().unwrap();
            guard.push(entry);
        }
        poll_fn(|cx| {
            let mut guard = waiter_state.lock().unwrap();
            if let Some(msg) = guard.message.take() {
                Poll::Ready(Ok(msg))
            } else {
                guard.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

#[async_trait]
impl<TResponse: Send> ListenerTrait<WebsocketMessageDto> for WebsocketListener<TResponse> {
    async fn on_message(&self, message: WebsocketMessageDto) -> EGResult<()> {
        let response = (self.converter)(message)?;
        let candidate = {
            let mut guard = self.state.lock().unwrap();
            let mut found = None;
            for i in 0..guard.len() {
                if (guard[i].filter)(&response) {
                    found = Some(guard.swap_remove(i));
                    break;
                }
            }
            found
        };
        if let Some(entry) = candidate {
            let mut state_guard = entry.state.lock().unwrap();
            state_guard.message = Some(response);
            if let Some(waker) = state_guard.waker.take() {
                waker.wake();
            } else {
                eprintln!("Message already sent");
            }
            Ok(())
        } else {
            self.delegate.on_message(response).await
        }
    }
}
