use crate::rate_limit::rate_limiter_state::RateLimiterState;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub interval_nanos: u128,
    pub capacity_per_interval: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            interval_nanos: Duration::from_mins(1).as_nanos(),
            capacity_per_interval: 10,
        }
    }
}

impl RateLimitConfig {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval_nanos = interval.as_nanos();
        self
    }
    pub fn with_capacity_per_interval(mut self, capacity_per_interval: u32) -> Self {
        assert!(capacity_per_interval > 0);
        self.capacity_per_interval = capacity_per_interval;
        self
    }
    pub(crate) fn to_state(&self) -> RateLimiterState {
        RateLimiterState::new(self.interval_nanos, self.capacity_per_interval)
    }
}
