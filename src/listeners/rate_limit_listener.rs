use crate::{
    error::EGResult,
    functions::ArcTryConvertRef,
    listeners::listener::ListenerTrait,
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
};
use async_trait::async_trait;
use std::sync::Arc;

/// A listener that feeds server-side rate-limit feedback into the local
/// limiter before forwarding the message to a delegate.
///
/// WebSocket API responses carry the current rate-limit usage (`rateLimits`)
/// on every message, and `exchangeInfo` returns the current limit
/// definitions, so this wrapper keeps the local model aligned with the server
/// even when no rate-limit headers are involved.
pub(crate) struct RateLimitFeedbackListener<T> {
    feedback: ArcTryConvertRef<T, RateLimitFeedback>,
    rate_limits: RateLimits,
    delegate: Arc<dyn ListenerTrait<TMessage = T>>,
}

impl<T> RateLimitFeedbackListener<T> {
    pub fn new(
        feedback: impl Fn(&T) -> EGResult<RateLimitFeedback> + Send + Sync + 'static,
        rate_limits: RateLimits,
        delegate: Arc<dyn ListenerTrait<TMessage = T>>,
    ) -> Self {
        Self {
            feedback: Arc::new(feedback),
            rate_limits,
            delegate,
        }
    }
}

#[async_trait]
impl<T> ListenerTrait for RateLimitFeedbackListener<T>
where
    T: Send,
{
    type TMessage = T;

    async fn on_message(&self, message: T) -> EGResult<()> {
        let feedback = (self.feedback)(&message)?;
        self.rate_limits.apply_feedback(&feedback)?;
        self.delegate.on_message(message).await
    }
}

impl<T> std::fmt::Display for RateLimitFeedbackListener<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitFeedbackListener")
            .field("feedback", &"<function>")
            .field("rate_limits", &self.rate_limits)
            .field("delegate", &"<Listener>")
            .finish()
    }
}
