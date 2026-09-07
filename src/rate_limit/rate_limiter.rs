use crate::{
    error::{EGError, EGResult},
    rate_limit::{rate_limit_config::RateLimitConfig, rate_limiter_state::RateLimiterState},
};
use exchange_types::rate_limited::{RateLimit, RateUsage};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug, Clone)]
pub(crate) struct RateLimiter {
    rate_limiters: Arc<Mutex<Vec<RateLimiterState>>>,
}

impl RateLimiter {
    pub fn new(rate_limits: Vec<RateLimitConfig>) -> Self {
        Self {
            rate_limiters: Arc::new(Mutex::new(
                rate_limits.iter().map(|rl| rl.to_state()).collect(),
            )),
        }
    }
    pub fn did_acquire(&self, cost: u32) -> EGResult<bool> {
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
    pub fn refund(&self, cost: u32) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            limiter.refund(cost);
        }
        Ok(())
    }
    pub fn set_usage(&self, usage: &HashMap<RateLimit, RateUsage>) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for (rate_limit, rate_limit_usage) in usage {
            for limiter in limiters_guard.iter_mut() {
                if limiter.restriction() == rate_limit.restriction
                    && limiter.interval_nanos() == rate_limit.interval_nanos as u128
                {
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
