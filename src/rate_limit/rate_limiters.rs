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
    pub fn did_acquire(&self, costs: &[(RateLimitRestriction, UsageCount)]) -> EGResult<bool> {
        let _guard = self
            .acquisition_lock
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        let mut acquired = Vec::with_capacity(costs.len());
        for &(restriction, cost) in costs {
            let did_acquire = match self.limiters.get(&restriction) {
                Some(limiter) => match limiter.did_acquire(cost) {
                    Ok(did_acquire) => did_acquire,
                    Err(error) => {
                        self.refund_acquired(&acquired);
                        return Err(error);
                    }
                },
                None => true,
            };
            if !did_acquire {
                self.refund_acquired(&acquired);
                return Ok(false);
            }
            acquired.push((restriction, cost));
        }
        Ok(true)
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
