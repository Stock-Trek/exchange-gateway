use crate::{
    error::EGResult,
    functions::ArcTryConvertValue,
    listeners::listener::{Listener, ListenerTrait},
    transports::transport::TransportMessageDto,
};
use async_trait::async_trait;
use std::{
    future::poll_fn,
    marker::PhantomData,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

#[derive(Clone)]
pub(crate) struct WebsocketListener<TTransportBody, TResponse>
where
    TResponse: Send,
{
    converter: ArcTryConvertValue<TransportMessageDto<TTransportBody>, TResponse>,
    delegate: Listener<TResponse>,
    handlers: Arc<Mutex<Vec<Arc<dyn MessageHandler<TResponse>>>>>,
}

impl<TTransportBody, TResponse> std::fmt::Debug for WebsocketListener<TTransportBody, TResponse>
where
    TResponse: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<function>")
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<MessageHandler>>")
            .finish()
    }
}

impl<TTransportBody, TResponse> WebsocketListener<TTransportBody, TResponse>
where
    TResponse: Send + Sync + 'static,
{
    pub fn new(
        converter: ArcTryConvertValue<TransportMessageDto<TTransportBody>, TResponse>,
        delegate: Listener<TResponse>,
    ) -> Self {
        Self {
            converter,
            delegate,
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[allow(unused)]
    pub async fn wait_for_response(&self) -> EGResult<TResponse> {
        self.wait_for_converted_response(Arc::new(|msg| Ok(msg)))
            .await
    }

    pub async fn wait_for_converted_response<TConvertedResponse>(
        &self,
        converter: ArcTryConvertValue<TResponse, TConvertedResponse>,
    ) -> EGResult<TConvertedResponse>
    where
        TConvertedResponse: Send + 'static,
    {
        let waiter_state = Arc::new(Mutex::new(WaiterState {
            converted_response: None,
            waker: None,
        }));
        let entry = Arc::new(WaitEntry {
            converter,
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
impl<TTransportBody, TResponse> ListenerTrait<TransportMessageDto<TTransportBody>>
    for WebsocketListener<TTransportBody, TResponse>
where
    TTransportBody: Send,
    TResponse: Clone + Send,
{
    async fn on_message(&self, message: TransportMessageDto<TTransportBody>) -> EGResult<()> {
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
        match (self.converter)(response) {
            Err(_) => false,
            Ok(converted_response) => {
                let mut state = self.state.lock().unwrap();
                state.converted_response = Some(converted_response);
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                true
            }
        }
    }
}

struct WaitEntry<TResponse, TConvertedResponse>
where
    TResponse: Send,
    TConvertedResponse: Send,
{
    converter: ArcTryConvertValue<TResponse, TConvertedResponse>,
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
