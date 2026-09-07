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
