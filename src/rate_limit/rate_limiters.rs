use crate::{error::EGResult, rate_limit::rate_limiter::RateLimiter};
use exchange_types::{
    new_types::{Nanoseconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction, RateUsage},
};
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Clone)]
pub struct RateLimiters {
    limiters: HashMap<RateLimitRestriction, RateLimiter>,
}

impl RateLimiters {
    pub fn new(limiters: HashMap<RateLimitRestriction, RateLimiter>) -> Self {
        Self { limiters }
    }
    /// Attempts to acquire `cost` for `restriction`.
    ///
    /// Returns `Ok(true)` when the cost was acquired, or when no limiter is
    /// configured for `restriction` (an unconfigured restriction is not rate
    /// limited). Returns `Ok(false)` only when a configured limiter rejects
    /// the cost because the rate limit is exhausted.
    pub fn did_acquire(
        &self,
        restriction: RateLimitRestriction,
        cost: UsageCount,
    ) -> EGResult<bool> {
        if let Some(limiter) = self.limiters.get(&restriction) {
            limiter.did_acquire(cost)
        } else {
            Ok(true)
        }
    }
    pub fn refund(&self, restriction: RateLimitRestriction, cost: UsageCount) -> EGResult<()> {
        if let Some(limiter) = self.limiters.get(&restriction) {
            limiter.refund(cost)
        } else {
            Ok(())
        }
    }
    pub fn set_usage(&self, usage: &HashMap<RateLimit, RateUsage>) -> EGResult<()> {
        for (restriction, limiter) in &self.limiters {
            let restriction_usage = usage
                .iter()
                .filter(|(rate_limit, _)| rate_limit.restriction == *restriction)
                .map(|(rate_limit, rate_usage)| (rate_limit.interval_nanos, *rate_usage))
                .collect::<HashMap<Nanoseconds, RateUsage>>();
            limiter.set_usage(&restriction_usage)?;
        }
        Ok(())
    }
    pub fn retry_after(&self, retry_after: Duration) -> EGResult<()> {
        for limiter in self.limiters.values() {
            limiter.throttle(retry_after)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::rate_limiter_state::RateLimiterState;
    use exchange_types::rate_limited::RateLimitRestriction;

    #[test]
    fn unconfigured_restriction_is_not_rate_limited() {
        let rate_limiters = RateLimiters::new(HashMap::new());
        for restriction in [
            RateLimitRestriction::Connection,
            RateLimitRestriction::OrderCount,
            RateLimitRestriction::RawRequests,
            RateLimitRestriction::Weight,
        ] {
            assert!(
                rate_limiters
                    .did_acquire(restriction, UsageCount::ONE)
                    .unwrap()
            );
            assert!(rate_limiters.refund(restriction, UsageCount::ONE).is_ok());
        }
    }

    #[test]
    fn configured_limiter_still_rejects_when_exhausted() {
        let state = RateLimiterState::new(Nanoseconds(1_000_000_000), UsageCount::ONE);
        let mut limiters = HashMap::new();
        limiters.insert(RateLimitRestriction::Weight, RateLimiter::new(&[state]));
        let rate_limiters = RateLimiters::new(limiters);
        assert!(
            rate_limiters
                .did_acquire(RateLimitRestriction::Weight, UsageCount::ONE)
                .unwrap()
        );
        assert!(
            !rate_limiters
                .did_acquire(RateLimitRestriction::Weight, UsageCount::ONE)
                .unwrap()
        );
        assert!(
            rate_limiters
                .did_acquire(RateLimitRestriction::Connection, UsageCount::ONE)
                .unwrap()
        );
    }
}
