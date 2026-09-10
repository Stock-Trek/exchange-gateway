use crate::{
    error::{EGError, EGResult},
    functions::ArcPredicate,
    panic_guard::PanicUtils,
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

type Handlers = Arc<Mutex<Vec<ResponseHandler>>>;

#[derive(Clone)]
pub struct WebsocketListener {
    handlers: Handlers,
}

impl WebsocketListener {
    pub(crate) fn new() -> Self {
        Self {
            handlers: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub(crate) async fn on_message(&self, message: serde_json::Value) -> EGResult<()> {
        let mut guard = self.handlers.lock().map_err(|_| EGError::MutexPoisoned)?;
        let mut handler_index = None;
        for (index, handler) in guard.iter().enumerate() {
            if handler.handle(&message)? {
                handler_index = Some(index);
                break;
            }
        }
        if let Some(index) = handler_index {
            guard.swap_remove(index);
        }
        Ok(())
    }
    pub(crate) fn waiter_for_filtered_response(
        &self,
        filter: ArcPredicate<serde_json::Value>,
    ) -> EGResult<WaiterForResponse> {
        let state = Arc::new(Mutex::new(WaiterState::default()));
        let handler = ResponseHandler {
            state: state.clone(),
            filter,
        };
        self.handlers
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .push(handler);
        Ok(WaiterForResponse {
            state,
            handlers: self.handlers.clone(),
        })
    }
}

impl Default for WebsocketListener {
    fn default() -> Self {
        Self::new()
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
    handlers: Handlers,
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
        if let Ok(mut handlers) = self.handlers.lock() {
            handlers.retain(|handler| !Arc::ptr_eq(&handler.state, &self.state));
        }
    }
}

struct ResponseHandler {
    state: Arc<Mutex<WaiterState>>,
    filter: ArcPredicate<serde_json::Value>,
}

impl ResponseHandler {
    fn handle(&self, response: &serde_json::Value) -> EGResult<bool> {
        let is_handled = match PanicUtils::catch_panic(|| (self.filter)(response)) {
            Ok(is_handled) => is_handled,
            Err(payload) => {
                let error = EGError::CallbackPanicked(PanicUtils::panic_message(payload.as_ref()));
                let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
                state.error = Some(error);
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
                return Ok(true);
            }
        };
        if is_handled {
            let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
            state.filtered_response = Some(response.clone());
            if let Some(waker) = state.waker.take() {
                waker.wake();
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, sync::Arc, task::Waker};

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn dropped_waiter_is_removed_from_listener() {
        let listener = WebsocketListener::new();
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|_| true))
            .unwrap();
        assert_eq!(listener.handlers.lock().unwrap().len(), 1);
        drop(waiter);
        assert_eq!(listener.handlers.lock().unwrap().len(), 0);
    }

    #[test]
    fn dropped_waiter_does_not_swallow_response() {
        let listener = WebsocketListener::new();
        let stale = listener
            .waiter_for_filtered_response(Arc::new(|_| true))
            .unwrap();
        drop(stale);
        let fresh = listener
            .waiter_for_filtered_response(Arc::new(|_| true))
            .unwrap();
        block_on(listener.on_message(serde_json::json!({ "value": 42 }))).unwrap();
        assert_eq!(block_on(fresh).unwrap(), serde_json::json!({ "value": 42 }));
    }
}
