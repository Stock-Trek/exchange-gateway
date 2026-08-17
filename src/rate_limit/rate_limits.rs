use crate::rate_limit::rate_limiter::RateLimiter;

#[derive(Debug, Clone)]
pub(crate) struct RateLimits {
    pub request: RateLimiter,
}
