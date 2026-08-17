use crate::rate_limit::rate_limiter::RateLimiter;

#[derive(Debug, Clone)]
pub(crate) struct RateLimits {
    /// Binance `REQUEST_WEIGHT` quota shared by all endpoints. Each request
    /// consumes its real documented weight (see the per-spec weight functions).
    pub request_weight: RateLimiter,
}
