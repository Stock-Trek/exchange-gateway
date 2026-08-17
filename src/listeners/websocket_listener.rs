use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;
use std::{
    future::poll_fn,
    marker::PhantomData,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

#[derive(Clone)]
pub(crate) struct WebsocketListener<TransportRes, EGRes> {
    converter: ArcTryConvertValue<TransportRes, EGRes>,
    delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    handlers: Arc<Mutex<Vec<Arc<dyn MessageHandler<EGRes>>>>>,
}

impl<TransportRes, EGRes> std::fmt::Debug for WebsocketListener<TransportRes, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<Converter>")
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<MessageHandler>>")
            .finish()
    }
}

impl<TransportRes, EGRes> WebsocketListener<TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub fn new(
        converter: impl Fn(TransportRes) -> EGResult<EGRes> + Send + Sync + 'static,
        delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    ) -> Self {
        Self {
            converter: Arc::new(converter),
            delegate,
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub async fn wait_for_filtered_response(&self, filter: ArcPredicate<EGRes>) -> EGResult<EGRes> {
        let waiter_state = Arc::new(Mutex::new(WaiterState::default()));
        let entry = Arc::new(WaitEntry {
            filter,
            state: waiter_state.clone(),
            _phantom_response: PhantomData,
        });
        {
            let mut guard = self.handlers.lock().map_err(|_| EGError::BadResponse)?;
            guard.push(entry);
        }
        poll_fn(|cx| {
            let mut guard = waiter_state.lock().map_err(|_| EGError::BadResponse)?;
            if let Some(msg) = guard.filtered_response.take() {
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
impl<TransportRes, EGRes> ListenerTrait for WebsocketListener<TransportRes, EGRes>
where
    EGRes: Clone + Send,
    TransportRes: Send,
{
    type TMessage = TransportRes;

    async fn on_message(&self, message: TransportRes) -> EGResult<()> {
        let response = (self.converter)(message)?;
        {
            let mut guard = self.handlers.lock().map_err(|_| EGError::BadResponse)?;
            let mut handler_index = None;
            for (index, handler) in guard.iter().enumerate() {
                if handler.clone().handle(response.clone())? {
                    handler_index = Some(index);
                    break;
                }
            }
            if let Some(index) = handler_index {
                guard.swap_remove(index);
                return Ok(());
            }
        }
        self.delegate.on_message(response).await
    }
}

trait MessageHandler<EGRes>: Send + Sync
where
    EGRes: Send,
{
    fn handle(self: Arc<Self>, response: EGRes) -> EGResult<bool>;
}

impl<TResponse> MessageHandler<TResponse> for WaitEntry<TResponse>
where
    TResponse: Send + Sync,
{
    fn handle(self: Arc<Self>, response: TResponse) -> EGResult<bool> {
        let is_handled = (self.filter)(&response);
        if is_handled {
            let mut state = self.state.lock().map_err(|_| EGError::BadResponse)?;
            state.filtered_response = Some(response);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
        Ok(is_handled)
    }
}

struct WaitEntry<TResponse> {
    filter: ArcPredicate<TResponse>,
    state: Arc<Mutex<WaiterState<TResponse>>>,
    _phantom_response: PhantomData<TResponse>,
}

struct WaiterState<EGRes> {
    filtered_response: Option<EGRes>,
    waker: Option<Waker>,
}

impl<EGRes> Default for WaiterState<EGRes>
where
    EGRes: Send,
{
    fn default() -> Self {
        Self {
            filtered_response: None,
            waker: None,
        }
    }
}
