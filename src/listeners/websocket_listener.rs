use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
    panic_guard::{catch_panic, panic_message},
};
use async_trait::async_trait;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

const MAX_PENDING_HANDLERS: usize = 1024;

#[derive(Clone)]
pub struct WebsocketListener<TransportRes, EGRes> {
    converter: ArcTryConvertValue<TransportRes, EGRes>,
    delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    handlers: Arc<Mutex<Vec<Arc<ResponseHandler<EGRes>>>>>,
}

impl<TransportRes, EGRes> WebsocketListener<TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub(crate) fn new(
        converter: ArcTryConvertValue<TransportRes, EGRes>,
        delegate: impl ListenerTrait<TMessage = EGRes> + 'static,
    ) -> Self {
        Self {
            converter,
            delegate: Arc::new(delegate),
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(crate) fn waiter_for_filtered_response(
        &self,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<WaiterForResponse<EGRes>> {
        let state = Arc::new(Mutex::new(WaiterState::default()));
        let handler = Arc::new(ResponseHandler {
            state: state.clone(),
            filter,
        });
        {
            let mut guard = self.handlers.lock().map_err(|_| EGError::MutexPoisoned)?;
            if guard.len() >= MAX_PENDING_HANDLERS {
                guard.retain(|existing| !existing.is_abandoned());
            }
            guard.push(handler);
        }
        Ok(WaiterForResponse { state })
    }
}

#[async_trait]
impl<TransportRes, EGRes> ListenerTrait for WebsocketListener<TransportRes, EGRes>
where
    EGRes: Clone + Send,
    TransportRes: Send,
{
    type TMessage = TransportRes;

    async fn on_connected(&self) -> EGResult<()> {
        self.delegate.on_connected().await
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        fail_pending_waiters(&self.handlers)?;
        self.delegate.on_disconnected().await
    }
    async fn on_error(&self, error: EGError) -> EGResult<()> {
        self.delegate.on_error(error).await
    }
    async fn on_message(&self, message: TransportRes) -> EGResult<()> {
        let response = match catch_panic(|| (self.converter)(message)) {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                self.delegate.on_error(error).await?;
                return Ok(());
            }
            Err(payload) => {
                let error = EGError::CallbackPanicked(panic_message(payload.as_ref()));
                self.delegate.on_error(error).await?;
                return Ok(());
            }
        };
        match remove_handler(&self.handlers, |handler| {
            handler.clone().handle(response.clone())
        }) {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => {
                self.delegate.on_error(error).await?;
                return Ok(());
            }
        }
        if let Err(error) = self.delegate.on_message(response).await {
            self.delegate.on_error(error).await?;
        }
        Ok(())
    }
}

impl<TransportRes, EGRes> std::fmt::Debug for WebsocketListener<TransportRes, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<Converter>")
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<ResponseHandler>>")
            .finish()
    }
}

fn fail_pending_waiters<EGRes>(handlers: &Mutex<Vec<Arc<ResponseHandler<EGRes>>>>) -> EGResult<()> {
    let mut guard = handlers.lock().map_err(|_| EGError::MutexPoisoned)?;
    for handler in guard.drain(..) {
        let mut state = handler.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        state.connection_lost = Some(EGError::NotConnected);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
    }
    Ok(())
}

pub(crate) struct WaiterForResponse<EGRes>
where
    EGRes: Send,
{
    state: Arc<Mutex<WaiterState<EGRes>>>,
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
        } else if let Some(error) = state.connection_lost.take() {
            Poll::Ready(Err(error))
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
        let _ = self.state.lock().map(|mut state| {
            state.abandoned = true;
            state.waker = None;
        });
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
}

impl<EGRes> ResponseHandler<EGRes> {
    fn is_abandoned(&self) -> bool {
        self.state.lock().is_ok_and(|state| state.abandoned)
    }

    fn handle(self: Arc<Self>, response: EGRes) -> EGResult<bool> {
        let is_handled = match catch_panic(|| (self.filter)(&response)) {
            Ok(is_handled) => is_handled,
            Err(payload) => {
                // The user's response matcher panicked. Fail the waiter that
                // owns it instead of unwinding while the handlers lock is held
                // (which would poison the mutex and break every subsequent
                // message). Claiming the response makes `remove_handler` drop
                // the broken handler so it is not invoked again.
                let error = EGError::CallbackPanicked(panic_message(payload.as_ref()));
                let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
                if !state.abandoned {
                    state.connection_lost = Some(error);
                    if let Some(waker) = state.waker.take() {
                        waker.wake();
                    }
                }
                return Ok(true);
            }
        };
        if is_handled {
            let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
            if !state.abandoned {
                state.filtered_response = Some(response);
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
            }
        }
        Ok(is_handled)
    }
}

struct WaiterState<EGRes> {
    filtered_response: Option<EGRes>,
    connection_lost: Option<EGError>,
    waker: Option<Waker>,
    abandoned: bool,
}

impl<EGRes> Default for WaiterState<EGRes>
where
    EGRes: Send,
{
    fn default() -> Self {
        Self {
            filtered_response: None,
            connection_lost: None,
            waker: None,
            abandoned: false,
        }
    }
}
