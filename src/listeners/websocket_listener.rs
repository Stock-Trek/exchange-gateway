use crate::{
    error::{EGError, EGResult},
    functions::ArcPredicate,
    listeners::listener::ListenerTrait,
    panic_guard::PanicUtils,
};
use async_trait::async_trait;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

const MAX_PENDING_HANDLERS: usize = 1024;

#[derive(Clone)]
pub struct WebsocketListener {
    delegate: Arc<dyn ListenerTrait<TMessage = serde_json::Value>>,
    handlers: Arc<Mutex<Vec<Arc<ResponseHandler>>>>,
}

impl WebsocketListener {
    pub(crate) fn new(
        delegate: impl ListenerTrait<TMessage = serde_json::Value> + 'static,
    ) -> Self {
        Self {
            delegate: Arc::new(delegate),
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(crate) fn waiter_for_filtered_response(
        &self,
        filter: ArcPredicate<serde_json::Value>,
    ) -> EGResult<WaiterForResponse> {
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
impl ListenerTrait for WebsocketListener {
    type TMessage = serde_json::Value;

    async fn on_connected(&self) -> EGResult<()> {
        self.delegate.on_connected().await
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        self.delegate.on_disconnected().await
    }
    async fn on_error(&self, error: EGError) -> EGResult<()> {
        self.delegate.on_error(error).await
    }
    async fn on_message(&self, message: serde_json::Value) -> EGResult<()> {
        match remove_handler(&self.handlers, |handler| {
            handler.clone().handle(message.clone())
        }) {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(error) => {
                self.delegate.on_error(error).await?;
                return Ok(());
            }
        }
        if let Err(error) = self.delegate.on_message(message).await {
            self.delegate.on_error(error).await?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for WebsocketListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<Converter>")
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<ResponseHandler>>")
            .finish()
    }
}

pub(crate) struct WaiterForResponse {
    state: Arc<Mutex<WaiterState>>,
}

impl Future for WaiterForResponse {
    type Output = EGResult<serde_json::Value>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(EGError::MutexPoisoned)),
        };
        if let Some(msg) = state.filtered_response.take() {
            Poll::Ready(Ok(msg))
        } else if let Some(error) = state.error.take() {
            Poll::Ready(Err(error))
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for WaiterForResponse {
    fn drop(&mut self) {
        let _ = self.state.lock().map(|mut state| {
            state.abandoned = true;
            state.waker = None;
        });
    }
}

fn remove_handler(
    handlers: &Mutex<Vec<Arc<ResponseHandler>>>,
    mut predicate: impl FnMut(&Arc<ResponseHandler>) -> EGResult<bool>,
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

struct ResponseHandler {
    state: Arc<Mutex<WaiterState>>,
    filter: ArcPredicate<serde_json::Value>,
}

impl ResponseHandler {
    fn is_abandoned(&self) -> bool {
        self.state.lock().is_ok_and(|state| state.abandoned)
    }

    fn handle(self: Arc<Self>, response: serde_json::Value) -> EGResult<bool> {
        let is_handled = match PanicUtils::catch_panic(|| (self.filter)(&response)) {
            Ok(is_handled) => is_handled,
            Err(payload) => {
                let error = EGError::CallbackPanicked(PanicUtils::panic_message(payload.as_ref()));
                let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
                if !state.abandoned {
                    state.error = Some(error);
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

#[derive(Default)]
struct WaiterState {
    filtered_response: Option<serde_json::Value>,
    error: Option<EGError>,
    waker: Option<Waker>,
    abandoned: bool,
}
