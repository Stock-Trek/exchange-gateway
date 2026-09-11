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
    pub fn did_acquire_all(&self, costs: &[(RateLimitRestriction, UsageCount)]) -> EGResult<bool> {
        let _guard = self
            .acquisition_lock
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        let mut acquired = Vec::with_capacity(costs.len());
        for &(restriction, cost) in costs {
            if !self.did_acquire(restriction, cost)? {
                for &(restriction, cost) in &acquired {
                    let _ = self.refund(restriction, cost);
                }
                return Ok(false);
            }
            acquired.push((restriction, cost));
        }
        Ok(true)
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

    #[test]
    fn did_acquire_all_refunds_on_partial_failure() {
        let order_interval = Nanoseconds(60_000_000_000);
        let weight_interval = Nanoseconds(60_000_000_000);
        let order_limiter =
            RateLimiter::new(&[RateLimiterState::new(order_interval, UsageCount(10))]);
        let weight_limiter =
            RateLimiter::new(&[RateLimiterState::new(weight_interval, UsageCount(1))]);
        let mut limiters = HashMap::new();
        limiters.insert(RateLimitRestriction::OrderCount, order_limiter);
        limiters.insert(RateLimitRestriction::Weight, weight_limiter);
        let limiters = RateLimiters::new(limiters);

        assert!(
            limiters
                .did_acquire(RateLimitRestriction::Weight, UsageCount(1))
                .unwrap()
        );

        let costs = [
            (RateLimitRestriction::OrderCount, UsageCount(1)),
            (RateLimitRestriction::Weight, UsageCount(1)),
        ];
        assert!(!limiters.did_acquire_all(&costs).unwrap());

        let remaining = limiters.remaining_capacity().unwrap();
        assert_eq!(
            remaining.get(&RateLimit {
                restriction: RateLimitRestriction::OrderCount,
                interval_nanos: order_interval,
            }),
            Some(&UsageCount(10)),
        );
    }
}
