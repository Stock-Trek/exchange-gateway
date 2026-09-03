use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertRef, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
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
    feedback: ArcTryConvertRef<TransportRes, RateLimitFeedback>,
    rate_limits: RateLimits,
    delegate: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    handlers: Arc<Mutex<Vec<Arc<ResponseHandler<EGRes>>>>>,
}

impl<TransportRes, EGRes> std::fmt::Debug for WebsocketListener<TransportRes, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketListener")
            .field("converter", &"<Converter>")
            .field("feedback", &"<function>")
            .field("rate_limits", &self.rate_limits)
            .field("delegate", &"<Listener>")
            .field("handlers", &"<Vec<ResponseHandler>>")
            .finish()
    }
}

impl<TransportRes, EGRes> WebsocketListener<TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub(crate) fn new(
        converter: ArcTryConvertValue<TransportRes, EGRes>,
        feedback: impl Fn(&TransportRes) -> EGResult<RateLimitFeedback> + Send + Sync + 'static,
        rate_limits: RateLimits,
        delegate: impl ListenerTrait<TMessage = EGRes> + 'static,
    ) -> Self {
        Self {
            converter,
            feedback: Arc::new(feedback),
            rate_limits,
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
        let feedback = match (self.feedback)(&message) {
            Ok(feedback) => feedback,
            Err(error) => {
                self.delegate.on_error(error).await?;
                return Ok(());
            }
        };
        if let Err(error) = self.rate_limits.apply_feedback(&feedback) {
            self.delegate.on_error(error).await?;
            return Ok(());
        }
        let response = match (self.converter)(message) {
            Ok(response) => response,
            Err(error) => {
                self.delegate.on_error(error).await?;
                return Ok(());
            }
        };
        match remove_handler(&self.handlers, |handler| {
            handler.clone().handle(response.clone(), &feedback)
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
        } else if let Some(error) = state.rate_limited.take() {
            Poll::Ready(Err(error))
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

    fn handle(self: Arc<Self>, response: EGRes, feedback: &RateLimitFeedback) -> EGResult<bool> {
        let is_handled = (self.filter)(&response);
        if is_handled {
            let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
            if !state.abandoned {
                if feedback.has_retry_feedback() {
                    state.rate_limited = Some(EGError::RateLimited(feedback.clone()));
                } else {
                    state.filtered_response = Some(response);
                }
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
    rate_limited: Option<EGError>,
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
            rate_limited: None,
            connection_lost: None,
            waker: None,
            abandoned: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::{
        feedback::RateLimitUsage, rate_limit_config::RateLimitConfig,
        rate_limit_type::RateLimitType, rate_limiter::RateLimiter,
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
                rate_limit_type: RateLimitType::RequestWeight,
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used: Some(message.used),
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

    #[derive(Default)]
    struct ErrorRecordingListener {
        received: Arc<Mutex<Vec<TestMessage>>>,
        errors: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ListenerTrait for ErrorRecordingListener {
        type TMessage = TestMessage;

        async fn on_message(&self, message: TestMessage) -> EGResult<()> {
            self.received
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?
                .push(message);
            Ok(())
        }

        async fn on_error(&self, error: EGError) -> EGResult<()> {
            self.errors
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?
                .push(error.to_string());
            Ok(())
        }
    }

    fn rate_limits() -> RateLimits {
        RateLimits {
            weight: RateLimiter::new(vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 100,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }]),
            orders: RateLimiter::new(vec![]),
        }
    }

    fn listener(
        delegate: impl ListenerTrait<TMessage = TestMessage> + 'static,
        limits: RateLimits,
        feedback: impl Fn(&TestMessage) -> EGResult<RateLimitFeedback> + Send + Sync + 'static,
    ) -> WebsocketListener<TestMessage, TestMessage> {
        WebsocketListener::new(Arc::new(Ok), feedback, limits, delegate)
    }

    #[tokio::test]
    async fn send_and_wait_matching_message_applies_feedback() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
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
    async fn send_and_wait_matching_message_with_retry_feedback_is_error() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), |message: &TestMessage| {
            Ok(RateLimitFeedback {
                retry_after: Some(Duration::from_secs(30)),
                usage: vec![RateLimitUsage {
                    rate_limit_type: RateLimitType::RequestWeight,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                    used: Some(message.used),
                    limit: None,
                }],
                ..Default::default()
            })
        });
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
            .unwrap();
        assert!(limits.weight.did_acquire(10).unwrap());
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        // A response with retry feedback is an error, not a success: the
        // waiter resolves with the server's feedback.
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should resolve")
            .expect_err("retry feedback should be an error");
        let feedback = match error {
            EGError::RateLimited(feedback) => feedback,
            other => panic!("expected RateLimited, got: {other:?}"),
        };
        assert_eq!(feedback.retry_after, Some(Duration::from_secs(30)));
        // The retry feedback drained the bucket until Retry-After elapses.
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[tokio::test]
    async fn partial_message_applies_feedback_and_forwards() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
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

    #[tokio::test]
    async fn waiters_fail_promptly_on_disconnect() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
            .unwrap();
        // Let the waiter register its waker, then drop the connection: the
        // waiter must be woken promptly with `NotConnected` instead of
        // sitting out the full timeout.
        let wait_task = tokio::spawn(waiter);
        tokio::task::yield_now().await;
        listener
            .on_disconnected()
            .await
            .expect("disconnect should succeed");
        let error = tokio::time::timeout(Duration::from_secs(1), wait_task)
            .await
            .expect("waiter should resolve promptly")
            .expect("wait task should not panic");
        assert!(matches!(error, Err(EGError::NotConnected)));
        // No message was forwarded to the delegate, and the drained handler
        // cannot consume a later response.
        assert!(received.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn timed_out_waiter_consumes_late_response_without_leaking_to_delegate() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
        // A send-and-wait times out (or is cancelled): the waiter is dropped
        // while the request may already be on the wire, so the matching
        // response that arrives afterwards must be consumed, not forwarded to
        // the delegate as if it were a push.
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
            .unwrap();
        drop(waiter);
        assert!(limits.weight.did_acquire(10).unwrap());
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        // The late response is swallowed: the delegate sees nothing ...
        assert!(received.lock().unwrap().is_empty());
        // ... but its rate-limit feedback is still applied, because the
        // exchange charged the request regardless of the local timeout.
        assert!(limits.weight.did_acquire(40).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[tokio::test]
    async fn timed_out_waiter_consumes_exactly_one_late_response() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
            .unwrap();
        drop(waiter);
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        assert!(received.lock().unwrap().is_empty());
        // The stale handler was removed once it consumed the late response,
        // so a later message is forwarded to the delegate normally (a retried
        // request uses a fresh id, but an unrelated message with a matching
        // filter must not be swallowed forever).
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        assert_eq!(
            *received.lock().unwrap(),
            vec![TestMessage { id: 7, used: 60 }]
        );
    }

    #[tokio::test]
    async fn abandoned_waiter_does_not_swallow_unrelated_messages() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
        let waiter = listener
            .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
            .unwrap();
        drop(waiter);
        // An abandoned waiter's filter only swallows its own response.
        listener
            .on_message(TestMessage { id: 1, used: 60 })
            .await
            .unwrap();
        assert_eq!(
            *received.lock().unwrap(),
            vec![TestMessage { id: 1, used: 60 }]
        );
    }

    #[tokio::test]
    async fn stale_handlers_are_evicted_when_the_cap_is_reached() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let delegate = RecordingListener {
            received: received.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
        for _ in 0..=MAX_PENDING_HANDLERS {
            let waiter = listener
                .waiter_for_filtered_response(Arc::new(|message: &TestMessage| message.id == 7))
                .unwrap();
            drop(waiter);
        }
        assert!(listener.handlers.lock().unwrap().len() <= MAX_PENDING_HANDLERS);
    }

    #[tokio::test]
    async fn conversion_failure_is_reported_through_on_error() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let delegate = ErrorRecordingListener {
            received: received.clone(),
            errors: errors.clone(),
        };
        let listener = listener(delegate, limits.clone(), |_message: &TestMessage| {
            Err(EGError::BadResponse)
        });
        // The message fails conversion: it is consumed, not forwarded ...
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        assert!(received.lock().unwrap().is_empty());
        // ... and the failure is sent through `on_error` instead of being
        // silently dropped.
        assert_eq!(
            *errors.lock().unwrap(),
            vec![EGError::BadResponse.to_string()]
        );
    }

    #[tokio::test]
    async fn feedback_failure_is_reported_through_on_error() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let delegate = ErrorRecordingListener {
            received: received.clone(),
            errors: errors.clone(),
        };
        let listener = listener(delegate, limits.clone(), |_message: &TestMessage| {
            Err(EGError::BadResponse)
        });
        // Feedback extraction fails: the message is consumed, not forwarded
        // ...
        listener
            .on_message(TestMessage { id: 7, used: 60 })
            .await
            .unwrap();
        assert!(received.lock().unwrap().is_empty());
        // ... and the failure is sent through `on_error` instead of being
        // silently dropped.
        assert_eq!(
            *errors.lock().unwrap(),
            vec![EGError::BadResponse.to_string()]
        );
    }

    #[tokio::test]
    async fn on_error_is_forwarded_to_the_delegate() {
        let limits = rate_limits();
        let received = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        let delegate = ErrorRecordingListener {
            received: received.clone(),
            errors: errors.clone(),
        };
        let listener = listener(delegate, limits.clone(), feedback);
        listener.on_error(EGError::NotConnected).await.unwrap();
        assert_eq!(
            *errors.lock().unwrap(),
            vec![EGError::NotConnected.to_string()]
        );
    }
}
