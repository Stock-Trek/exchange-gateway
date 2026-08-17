use crate::rate_limit::rate_limiter_state::RateLimiterState;

#[derive(Debug, Clone)]
pub(crate) struct RateLimitConfig {
    pub interval_nanos: u128,
    pub capacity_per_interval: u32,
}

impl RateLimitConfig {
    pub fn to_state(&self) -> RateLimiterState {
        RateLimiterState::new(self.interval_nanos, self.capacity_per_interval)
    }
}
