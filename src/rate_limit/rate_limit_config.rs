use crate::{
    clock::Clock,
    rate_limit::{rate_limit_type::RateLimitType, rate_limiter_state::RateLimiterState},
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct RateLimitConfig {
    pub rate_limit_type: RateLimitType,
    pub interval_nanos: u128,
    pub capacity_per_interval: u32,
}

impl RateLimitConfig {
    pub fn to_state(&self, clock: Arc<Clock>) -> RateLimiterState {
        RateLimiterState::new(
            clock,
            self.rate_limit_type,
            self.interval_nanos,
            self.capacity_per_interval,
        )
    }
}
