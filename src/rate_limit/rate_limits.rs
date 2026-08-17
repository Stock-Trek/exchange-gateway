use crate::rate_limit::rate_limiter::RateLimiter;

#[derive(Debug, Clone)]
pub(crate) struct RateLimits {
    #[allow(unused)]
    pub send_order_request: RateLimiter,
}
