use crate::{error::EGResult, rate_limit::rate_limiter::RateLimiter};
use exchange_types::rate_limited::{RateLimit, RateLimitRestriction, RateUsage};
use std::{collections::HashMap, time::Duration};

#[derive(Debug, Clone)]
pub(crate) struct RateLimits {
    limiters: HashMap<RateLimitRestriction, RateLimiter>,
}

impl RateLimits {
    pub fn did_acquire(&self, restriction: RateLimitRestriction, cost: u32) -> EGResult<bool> {
        if let Some(limiter) = self.limiters.get(&restriction) {
            limiter.did_acquire(cost)
        } else {
            Ok(false)
        }
    }
    pub fn refund(&self, restriction: RateLimitRestriction, cost: u32) -> EGResult<()> {
        if let Some(limiter) = self.limiters.get(&restriction) {
            limiter.refund(cost)
        } else {
            Ok(())
        }
    }
    pub fn set_usage(&self, usage: &HashMap<RateLimit, RateUsage>) -> EGResult<()> {
        for limiter in self.limiters.values() {
            limiter.set_usage(usage)?;
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
