use crate::{
    auth_gate::AuthGate,
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
};
use async_trait::async_trait;
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

#[derive(Clone)]
pub(crate) struct WebsocketListener<TransportRes, EGRes> {
    converter: ArcTryConvertValue<TransportRes, EGRes>,
    delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    handlers: Arc<Mutex<Vec<Arc<ResponseHandler<EGRes>>>>>,
    next_handler_id: Arc<AtomicU64>,
    auth_gate: Arc<AuthGate>,
}

impl<TransportRes, EGRes> std::fmt::Debug for WebsocketListener<TransportRes, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<Converter>")
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<ResponseHandler>>")
            .field("next_handler_id", &self.next_handler_id)
            .field("auth_gate", &self.auth_gate)
            .finish()
    }
}

impl<TransportRes, EGRes> WebsocketListener<TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub fn new(
        converter: ArcTryConvertValue<TransportRes, EGRes>,
        delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
        auth_gate: Arc<AuthGate>,
    ) -> Self {
        Self {
            converter,
            delegate,
            handlers: Arc::new(Mutex::new(Vec::new())),
            next_handler_id: Arc::new(AtomicU64::new(0)),
            auth_gate,
        }
    }
    pub fn waiter_for_filtered_response(
        &self,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<WaiterForResponse<EGRes>> {
        let handler_id = self.next_handler_id.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(Mutex::new(WaiterState::default()));
        let handler = Arc::new(ResponseHandler {
            state: state.clone(),
            filter,
            handler_id,
        });
        {
            let mut guard = self.handlers.lock().map_err(|_| EGError::MutexPoisoned)?;
            guard.push(handler);
        }
        Ok(WaiterForResponse {
            state,
            handlers: self.handlers.clone(),
            handler_id,
        })
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
        if remove_handler(&self.handlers, |handler| {
            handler.clone().handle(response.clone())
        })? {
            return Ok(());
        }
        self.delegate.on_message(response).await
    }

    async fn on_connected(&self) -> EGResult<()> {
        self.auth_gate.on_connection_established()?;
        self.delegate.on_connected().await
    }
}

pub(crate) struct WaiterForResponse<EGRes>
where
    EGRes: Send,
{
    state: Arc<Mutex<WaiterState<EGRes>>>,
    handlers: Arc<Mutex<Vec<Arc<ResponseHandler<EGRes>>>>>,
    handler_id: u64,
}

impl<EGRes> Future for WaiterForResponse<EGRes>
where
    EGRes: Send,
{
    type Output = EGResult<EGRes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(EGError::MutexPoisoned)),
        };
        if let Some(msg) = state.filtered_response.take() {
            Poll::Ready(Ok(msg))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<EGRes> Drop for WaiterForResponse<EGRes>
where
    EGRes: Send,
{
    fn drop(&mut self) {
        let _ = remove_handler(
            &self.handlers,
            |handler| Ok(handler.id() == self.handler_id),
        );
    }
}

fn remove_handler<EGRes>(
    handlers: &Mutex<Vec<Arc<ResponseHandler<EGRes>>>>,
    mut predicate: impl FnMut(&Arc<ResponseHandler<EGRes>>) -> EGResult<bool>,
) -> EGResult<bool> {
    let mut guard = handlers.lock().map_err(|_| EGError::MutexPoisoned)?;
    let mut handler_index = None;
    for (index, handler) in guard.iter().enumerate() {
        if predicate(handler)? {
            handler_index = Some(index);
            break;
        }
    }
    if let Some(index) = handler_index {
        guard.swap_remove(index);
        Ok(true)
    } else {
        Ok(false)
    }
}

struct ResponseHandler<EGRes> {
    state: Arc<Mutex<WaiterState<EGRes>>>,
    filter: ArcPredicate<EGRes>,
    handler_id: u64,
}

impl<EGRes> ResponseHandler<EGRes> {
    fn handle(self: Arc<Self>, response: EGRes) -> EGResult<bool> {
        let is_handled = (self.filter)(&response);
        if is_handled {
            let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
            state.filtered_response = Some(response);
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        }
        Ok(is_handled)
    }
    fn id(&self) -> u64 {
        self.handler_id
    }
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
