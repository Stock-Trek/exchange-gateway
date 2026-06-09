use crate::rate_limit::multi_rate_limiter::MultiRateLimiter;

pub struct RequestWeights {
    pub send_order_request: u32,
}

pub struct RateLimits {
    pub send_order_request: MultiRateLimiter,
}
