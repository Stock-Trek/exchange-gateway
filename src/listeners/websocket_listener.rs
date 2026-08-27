use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertRef, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
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
    feedback: ArcTryConvertRef<TransportRes, RateLimitFeedback>,
    rate_limits: RateLimits,
    delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    handlers: Arc<Mutex<Vec<Arc<ResponseHandler<EGRes>>>>>,
    next_handler_id: Arc<AtomicU64>,
}

impl<TransportRes, EGRes> std::fmt::Debug for WebsocketListener<TransportRes, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<Converter>")
            .field("feedback", &"<function>")
            .field("rate_limits", &self.rate_limits)
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<ResponseHandler>>")
            .field("next_handler_id", &self.next_handler_id)
            .finish()
    }
}

impl<TransportRes, EGRes> WebsocketListener<TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub fn new(
        converter: ArcTryConvertValue<TransportRes, EGRes>,
        feedback: impl Fn(&TransportRes) -> EGResult<RateLimitFeedback> + Send + Sync + 'static,
        rate_limits: RateLimits,
        delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    ) -> Self {
        Self {
            converter,
            feedback: Arc::new(feedback),
            rate_limits,
            delegate,
            handlers: Arc::new(Mutex::new(Vec::new())),
            next_handler_id: Arc::new(AtomicU64::new(0)),
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

#[async_trait]
impl<TransportRes, EGRes> ListenerTrait for WebsocketListener<TransportRes, EGRes>
where
    EGRes: Clone + Send,
    TransportRes: Send,
{
    type TMessage = TransportRes;

    async fn on_message(&self, message: TransportRes) -> EGResult<()> {
        // Every incoming message carries the server's view of the rate-limit
        // buckets (Binance's WebSocket API includes a `rateLimits` array on
        // each response), so feedback is applied before handler dispatch.
        // This covers both fire-and-forget messages (forwarded below) and
        // send-and-wait responses (matched by a handler, which would
        // otherwise short-circuit before the feedback listener).
        let feedback = (self.feedback)(&message)?;
        self.rate_limits.apply_feedback(&feedback)?;
        let response = (self.converter)(message)?;
        if remove_handler(&self.handlers, |handler| {
            handler.clone().handle(response.clone())
        })? {
            return Ok(());
        }
        self.delegate.on_message(response).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::{
        feedback::RateLimitUsage, rate_limit_config::RateLimitConfig, rate_limiter::RateLimiter,
    };
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMessage {
        id: u64,
        used: u32,
    }

    /// Reports the message's `used` value as the request-weight bucket usage,
    /// mirroring how Binance's WebSocket API reports `rateLimits` on every
    /// response.
    fn feedback(message: &TestMessage) -> EGResult<RateLimitFeedback> {
        Ok(RateLimitFeedback {
            usage: vec![RateLimitUsage {
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used: message.used,
                limit: None,
            }],
            ..Default::default()
        })
    }

    #[derive(Default)]
    struct RecordingListener {
        received: Arc<Mutex<Vec<TestMessage>>>,
    }

    #[async_trait]
    impl ListenerTrait for RecordingListener {
        type TMessage = TestMessage;

        async fn on_message(&self, message: TestMessage) -> EGResult<()> {
            self.received
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?
                .push(message);
            Ok(())
        }
    }

    fn rate_limits() -> RateLimits {
        RateLimits {
            weight: RateLimiter::new(vec![RateLimitConfig {
                capacity_per_interval: 100,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }]),
            orders: RateLimiter::new(vec![]),
        }
    }

    #[tokio::test]
    async fn send_and_wait_matching_message_applies_feedback() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate: Arc<dyn ListenerTrait<TMessage = TestMessage>> =
            Arc::new(RecordingListener {
                received: received.clone(),
            });
        let listener = WebsocketListener::new(Arc::new(Ok), feedback, limits.clone(), delegate);
        // A waiter (send-and-wait) is registered and the message matches its
        // filter, so the response is returned to the waiter rather than
        // forwarded to the delegate.
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
            .unwrap();
        assert!(limits.weight.did_acquire(10).unwrap());
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should resolve")
            .unwrap();
        assert_eq!(response, TestMessage { id: 7, used: 60 });
        // The matching response was handled by the waiter, so it must not be
        // forwarded to the delegate ...
        assert!(received.lock().unwrap().is_empty());
        // ... but its rate-limit feedback must still have been applied: the
        // bucket is trimmed to 100 - 60 = 40 remaining.
        assert!(limits.weight.did_acquire(40).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[tokio::test]
    async fn fire_and_forget_message_applies_feedback_and_forwards() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate: Arc<dyn ListenerTrait<TMessage = TestMessage>> =
            Arc::new(RecordingListener {
                received: received.clone(),
            });
        let listener = WebsocketListener::new(Arc::new(Ok), feedback, limits.clone(), delegate);
        assert!(limits.weight.did_acquire(10).unwrap());
        listener
            .on_message(TestMessage { id: 1, used: 60 })
            .await
            .unwrap();
        // No waiter matched, so the message is forwarded to the delegate ...
        assert_eq!(
            *received.lock().unwrap(),
            vec![TestMessage { id: 1, used: 60 }]
        );
        // ... and feedback is applied on the way through.
        assert!(limits.weight.did_acquire(40).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
    }
}
