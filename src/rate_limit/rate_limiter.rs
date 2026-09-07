use crate::{
    error::{EGError, EGResult},
    rate_limit::rate_limiter_state::RateLimiterState,
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
    pub fn did_acquire(&self, cost: UsageCount) -> EGResult<bool> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for (index, limiter) in limiters_guard.iter_mut().enumerate() {
            if !limiter.did_consume(cost) {
                for i in 0..index {
                    let limiter = &mut limiters_guard[i];
                    limiter.refund(cost);
                }
                return Ok(false);
            }
        }
        Ok(true)
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
