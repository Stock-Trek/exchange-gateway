use crate::rate_limit::{rate_limit_type::RateLimitType, rate_limiter_state::RateLimiterState};
#[cfg(test)]
use std::{sync::Arc, time::Instant};

#[derive(Debug, Clone)]
pub(crate) struct RateLimitConfig {
    pub rate_limit_type: RateLimitType,
    pub interval_nanos: u128,
    pub capacity_per_interval: u32,
}

impl RateLimitConfig {
    pub fn to_state(&self) -> RateLimiterState {
        RateLimiterState::new(
            self.rate_limit_type,
            self.interval_nanos,
            self.capacity_per_interval,
        )
    }
    /// Like [`Self::to_state`], but reading the clock through `now` so tests
    /// can drive time-dependent behaviour deterministically.
    #[cfg(test)]
    pub(crate) fn to_state_with_clock(
        &self,
        now: Arc<dyn Fn() -> Instant + Send + Sync>,
    ) -> RateLimiterState {
        RateLimiterState::with_clock(
            self.rate_limit_type,
            self.interval_nanos,
            self.capacity_per_interval,
            now,
        )
    }
}
