use crate::{
    error::{EGError, EGResult},
    rate_limit::rate_limiter::RateLimiter,
};
use exchange_types::{
    new_types::{Nanoseconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction, RateUsage},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct RateLimiters {
    limiters: HashMap<RateLimitRestriction, RateLimiter>,
    acquisition_lock: Arc<Mutex<()>>,
}

impl RateLimiters {
    pub fn new(limiters: HashMap<RateLimitRestriction, RateLimiter>) -> Self {
        Self {
            limiters,
            acquisition_lock: Arc::new(Mutex::new(())),
        }
    }
    pub fn remaining_capacity(&self) -> EGResult<HashMap<RateLimit, UsageCount>> {
        let mut capacities = HashMap::new();
        for (restriction, limiter) in &self.limiters {
            for (interval_nanos, remaining) in limiter.remaining_capacity()? {
                capacities.insert(
                    RateLimit {
                        restriction: *restriction,
                        interval_nanos,
                    },
                    remaining,
                );
            }
        }
        Ok(capacities)
    }
    pub fn did_acquire(&self, costs: &[(RateLimitRestriction, UsageCount)]) -> EGResult<()> {
        let _guard = self
            .acquisition_lock
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in self.limiters.values() {
            if limiter.is_throttled()? {
                return Err(EGError::RateLimited);
            }
        }
        let mut acquired = Vec::with_capacity(costs.len());
        for &(restriction, cost) in costs {
            if let Some(limiter) = self.limiters.get(&restriction) {
                if let Err(error) = limiter.did_acquire(cost) {
                    self.refund_acquired(&acquired);
                    return Err(error);
                }
                acquired.push((restriction, cost));
            }
        }
        Ok(())
    }
    fn refund_acquired(&self, acquired: &[(RateLimitRestriction, UsageCount)]) {
        for &(restriction, cost) in acquired {
            let _ = self.refund(restriction, cost);
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
    pub fn set_retry_after(&self, retry_after: Duration) -> EGResult<()> {
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

    fn rate_limiters() -> RateLimiters {
        let state = RateLimiterState::try_new(Nanoseconds(60_000_000_000), UsageCount(10))
            .expect("valid rate limiter state");
        RateLimiters::new(HashMap::from([(
            RateLimitRestriction::RawRequests,
            RateLimiter::new(&[state]),
        )]))
    }

    #[test]
    fn retry_after_blocks_zero_cost_requests() {
        let rate_limiters = rate_limiters();
        rate_limiters
            .set_retry_after(Duration::from_secs(60))
            .expect("set retry after");
        assert!(matches!(
            rate_limiters.did_acquire(&[]),
            Err(EGError::RateLimited)
        ));
    }

    #[test]
    fn zero_cost_requests_are_allowed_without_retry_after() {
        let rate_limiters = rate_limiters();
        assert!(rate_limiters.did_acquire(&[]).is_ok());
    }
}
