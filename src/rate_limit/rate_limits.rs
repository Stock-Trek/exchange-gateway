use crate::rate_limit::multi_rate_limiter::MultiRateLimiter;

#[derive(Debug, Clone)]
pub struct RateLimits {
    pub send_order_request: MultiRateLimiter,
}
