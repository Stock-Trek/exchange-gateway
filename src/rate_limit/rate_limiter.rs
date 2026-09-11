use crate::{
    error::{EGError, EGResult},
    rate_limit::rate_limiter_state::{AcquireResult, RateLimiterState},
};
use exchange_types::{
    new_types::{Nanoseconds, UsageCount},
    rate_limited::RateUsage,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct RateLimiter {
    rate_limiters: Arc<Mutex<Vec<RateLimiterState>>>,
}

impl RateLimiter {
    pub fn new(states: &[RateLimiterState]) -> Self {
        Self {
            rate_limiters: Arc::new(Mutex::new(states.to_vec())),
        }
    }
    pub fn did_acquire(&self, cost: UsageCount) -> EGResult<AcquireResult> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        let exceeded_capacity = limiters_guard
            .iter()
            .filter(|limiter| limiter.cost_exceeds_capacity(cost))
            .min_by_key(|limiter| limiter.capacity_per_interval())
            .map(|limiter| (limiter.capacity_per_interval(), limiter.interval_nanos()));
        if let Some((capacity, interval_nanos)) = exceeded_capacity {
            return Ok(AcquireResult::ExceedsCapacity {
                cost,
                capacity,
                interval_nanos,
            });
        }
        for (index, limiter) in limiters_guard.iter_mut().enumerate() {
            if limiter.did_consume(cost) != AcquireResult::Acquired {
                for i in 0..index {
                    let limiter = &mut limiters_guard[i];
                    limiter.refund(cost);
                }
                return Ok(AcquireResult::RateLimited);
            }
        }
        Ok(AcquireResult::Acquired)
    }
    pub fn remaining_capacity(&self) -> EGResult<HashMap<Nanoseconds, UsageCount>> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        Ok(limiters_guard
            .iter_mut()
            .map(|limiter| (limiter.interval_nanos(), limiter.remaining_capacity()))
            .collect())
    }
    pub fn refund(&self, cost: UsageCount) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            limiter.refund(cost);
        }
        Ok(())
    }
    pub fn set_usage(&self, interval_usage: &HashMap<Nanoseconds, RateUsage>) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for (interval_nanos, rate_limit_usage) in interval_usage {
            for limiter in limiters_guard.iter_mut() {
                if limiter.interval_nanos() == *interval_nanos {
                    limiter.sync_usage(rate_limit_usage.used, rate_limit_usage.limit);
                }
            }
        }
        Ok(())
    }
    pub fn throttle(&self, retry_after: Duration) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            limiter.throttle_after(retry_after);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: Nanoseconds = Nanoseconds(1_000_000_000);

    #[test]
    fn acquire_within_capacity_succeeds() {
        let limiter = RateLimiter::new(&[RateLimiterState::new(INTERVAL, UsageCount(10))]);
        assert_eq!(
            limiter.did_acquire(UsageCount(5)).unwrap(),
            AcquireResult::Acquired
        );
    }

    #[test]
    fn acquire_over_capacity_is_impossible() {
        let limiter = RateLimiter::new(&[RateLimiterState::new(INTERVAL, UsageCount(10))]);
        assert_eq!(
            limiter.did_acquire(UsageCount(11)).unwrap(),
            AcquireResult::ExceedsCapacity {
                capacity: UsageCount(10)
            }
        );
    }

    #[test]
    fn acquire_from_empty_bucket_is_temporarily_rate_limited() {
        let limiter = RateLimiter::new(&[RateLimiterState::new(INTERVAL, UsageCount(10))]);
        assert_eq!(
            limiter.did_acquire(UsageCount(10)).unwrap(),
            AcquireResult::Acquired
        );
        assert_eq!(
            limiter.did_acquire(UsageCount(1)).unwrap(),
            AcquireResult::RateLimited
        );
    }

    #[test]
    fn exceeds_capacity_takes_precedence_over_temporary_limit() {
        let limiter = RateLimiter::new(&[
            RateLimiterState::new(Nanoseconds(1_000_000_000), UsageCount(10)),
            RateLimiterState::new(Nanoseconds(2_000_000_000), UsageCount(5)),
        ]);
        assert_eq!(
            limiter.did_acquire(UsageCount(6)).unwrap(),
            AcquireResult::ExceedsCapacity {
                capacity: UsageCount(5)
            }
        );
    }
}
