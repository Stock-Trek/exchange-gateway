use crate::{
    error::EGResult,
    functions::{ResponseConverter, ResponsePredicate, TryConvertResponseFrom},
    listeners::listener::{Listener, ListenerTrait},
    transports::websocket::WebsocketMessageDto,
};
use async_trait::async_trait;
use std::{
    future::poll_fn,
    marker::PhantomData,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

pub struct WebsocketListener<TResponse>
where
    TResponse: Send,
{
    converter: TryConvertResponseFrom<WebsocketMessageDto, TResponse>,
    delegate: Listener<TResponse>,
    handlers: Arc<Mutex<Vec<Arc<dyn MessageHandler<TResponse>>>>>,
}

impl<TResponse> WebsocketListener<TResponse>
where
    TResponse: Send + Sync + 'static,
{
    pub fn new(
        converter: TryConvertResponseFrom<WebsocketMessageDto, TResponse>,
        delegate: Listener<TResponse>,
    ) -> Self {
        Self {
            converter,
            delegate,
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn wait_for_response(
        &self,
        filter: ResponsePredicate<TResponse>,
    ) -> EGResult<TResponse> {
        self.wait_for_converted_response(filter, Box::new(|msg| msg))
            .await
    }

    pub async fn wait_for_converted_response<TConvertedResponse>(
        &self,
        filter: ResponsePredicate<TResponse>,
        map: ResponseConverter<TResponse, TConvertedResponse>,
    ) -> EGResult<TConvertedResponse>
    where
        TConvertedResponse: Send + 'static,
    {
        let waiter_state = Arc::new(Mutex::new(WaiterState {
            converted_response: None,
            waker: None,
        }));
        let entry = Arc::new(WaitEntry {
            filter,
            converter: Mutex::new(Some(map)),
            state: waiter_state.clone(),
            _phantom_response: PhantomData,
        });
        {
            let mut guard = self.handlers.lock().unwrap();
            guard.push(entry);
        }
        poll_fn(|cx| {
            let mut guard = waiter_state.lock().unwrap();
            if let Some(msg) = guard.converted_response.take() {
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
impl<TResponse> ListenerTrait<WebsocketMessageDto> for WebsocketListener<TResponse>
where
    TResponse: Clone + Send,
{
    async fn on_message(&self, message: WebsocketMessageDto) -> EGResult<()> {
        let response = (self.converter)(message)?;
        {
            let mut guard = self.handlers.lock().unwrap();
            let handler_index = guard
                .iter()
                .position(|handler| handler.clone().handle(response.clone()));
            if let Some(index) = handler_index {
                guard.swap_remove(index);
                return Ok(());
            }
        }
        self.delegate.on_message(response).await
    }
}

trait MessageHandler<TResponse>: Send + Sync
where
    TResponse: Send,
{
    fn handle(self: Arc<Self>, response: TResponse) -> bool;
}

impl<TResponse, TConvertedResponse> MessageHandler<TResponse>
    for WaitEntry<TResponse, TConvertedResponse>
where
    TResponse: Send + Sync,
    TConvertedResponse: Send,
{
    fn handle(self: Arc<Self>, response: TResponse) -> bool {
        if (self.filter)(&response) {
            let converter = self
                .converter
                .lock()
                .unwrap()
                .take()
                .expect("converter already taken");
            let converted_response = converter(response);
            let mut state = self.state.lock().unwrap();
            state.converted_response = Some(converted_response);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
            true
        } else {
            false
        }
    }
}

struct WaitEntry<TResponse, TConvertedResponse>
where
    TResponse: Send,
    TConvertedResponse: Send,
{
    filter: ResponsePredicate<TResponse>,
    converter: Mutex<Option<ResponseConverter<TResponse, TConvertedResponse>>>,
    state: Arc<Mutex<WaiterState<TConvertedResponse>>>,
    _phantom_response: PhantomData<TResponse>,
}

struct WaiterState<TConvertedResponse>
where
    TConvertedResponse: Send,
{
    converted_response: Option<TConvertedResponse>,
    waker: Option<Waker>,
}
