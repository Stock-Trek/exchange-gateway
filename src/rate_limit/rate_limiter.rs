use crate::{
    error::{EGError, EGResult},
    rate_limit::{
        feedback::RateLimitUsage, rate_limit_config::RateLimitConfig,
        rate_limiter_state::RateLimiterState,
    },
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
    /// Realigns every bucket with the given interval to server-reported usage.
    ///
    /// Buckets with a different interval are left untouched (e.g. the daily
    /// request-weight bucket Binance reports is not modelled locally).
    pub fn apply_usage(&self, usage: &RateLimitUsage) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            if limiter.interval_nanos() == usage.interval_nanos {
                limiter.sync_usage(usage.used, usage.limit);
            }
        }
        Ok(())
    }
    /// Drains every bucket until `retry_after` elapses (a short default when
    /// the server did not send a `Retry-After` header).
    pub fn throttle(&self, retry_after: Option<Duration>) -> EGResult<()> {
        let until = Instant::now() + retry_after.unwrap_or(Duration::from_secs(1));
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            limiter.throttle(until);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::rate_limit_config::RateLimitConfig;
    use std::time::Duration;

    #[test]
    fn refund_returns_consumed_capacity() {
        let limiter = RateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 10,
            interval_nanos: Duration::from_secs(60).as_nanos(),
        }]);
        assert!(limiter.did_acquire(10).unwrap());
        assert!(!limiter.did_acquire(1).unwrap());
        limiter.refund(10).unwrap();
        assert!(limiter.did_acquire(1).unwrap());
    }

    #[test]
    fn refund_never_exceeds_capacity() {
        let limiter = RateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 10,
            interval_nanos: Duration::from_secs(60).as_nanos(),
        }]);
        assert!(limiter.did_acquire(1).unwrap());
        limiter.refund(10).unwrap();
        limiter.refund(10).unwrap();
        assert!(limiter.did_acquire(10).unwrap());
        assert!(!limiter.did_acquire(1).unwrap());
    }
}
