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
    /// Realigns every bucket of the same rate-limit type and interval with
    /// server-reported usage.
    ///
    /// Buckets with a different type or interval are left untouched: Binance
    /// reports `REQUEST_WEIGHT` (6000/min) and `RAW_REQUESTS` (61000/min)
    /// with the same one-minute window, so matching on interval alone would
    /// let the wrong usage overwrite the weight limiter's capacity.
    pub fn apply_usage(&self, usage: &RateLimitUsage) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            if limiter.rate_limit_type() == usage.rate_limit_type
                && limiter.interval_nanos() == usage.interval_nanos
            {
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
    use crate::rate_limit::{
        feedback::RateLimitUsage, rate_limit_config::RateLimitConfig,
        rate_limit_type::RateLimitType,
    };
    use std::time::Duration;

    #[test]
    fn refund_returns_consumed_capacity() {
        let limiter = RateLimiter::new(vec![RateLimitConfig {
            rate_limit_type: RateLimitType::RequestWeight,
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
            rate_limit_type: RateLimitType::RequestWeight,
            capacity_per_interval: 10,
            interval_nanos: Duration::from_secs(60).as_nanos(),
        }]);
        assert!(limiter.did_acquire(1).unwrap());
        limiter.refund(10).unwrap();
        limiter.refund(10).unwrap();
        assert!(limiter.did_acquire(10).unwrap());
        assert!(!limiter.did_acquire(1).unwrap());
    }

    #[test]
    fn apply_usage_matches_type_not_just_interval() {
        // Binance reports REQUEST_WEIGHT (6000/min) and RAW_REQUESTS
        // (61000/min) with the same 60s window, RAW_REQUESTS last. The
        // raw-requests usage must not overwrite the weight bucket's
        // capacity or limit.
        let limiter = RateLimiter::new(vec![RateLimitConfig {
            rate_limit_type: RateLimitType::RequestWeight,
            capacity_per_interval: 6000,
            interval_nanos: Duration::from_secs(60).as_nanos(),
        }]);
        limiter
            .apply_usage(&RateLimitUsage {
                rate_limit_type: RateLimitType::RequestWeight,
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used: 1000,
                limit: Some(6000),
            })
            .unwrap();
        limiter
            .apply_usage(&RateLimitUsage {
                rate_limit_type: RateLimitType::RawRequests,
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used: 60_000,
                limit: Some(61_000),
            })
            .unwrap();
        // The weight bucket keeps its 6000 limit with 5000 remaining.
        assert!(limiter.did_acquire(5000).unwrap());
        assert!(!limiter.did_acquire(1).unwrap());
    }

    #[test]
    fn apply_usage_matches_interval_within_a_type() {
        // The two order buckets share the ORDERS type but differ in
        // interval: daily usage must not realign the 10-second bucket.
        let limiter = RateLimiter::new(vec![
            RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 50,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            },
            RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 160_000,
                interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
            },
        ]);
        // Use 5 of the 10-second budget.
        assert!(limiter.did_acquire(5).unwrap());
        // The server reports daily usage: 5000 used of a 10000 limit. Only
        // the daily bucket is realigned (5000 remaining); the 10-second
        // bucket keeps its 45 remaining, so total consumption stays capped
        // at 45. If the daily usage were misapplied to the 10-second bucket
        // it would gain 5000 remaining and the extra order would pass.
        limiter
            .apply_usage(&RateLimitUsage {
                rate_limit_type: RateLimitType::Orders,
                interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                used: 5000,
                limit: Some(10_000),
            })
            .unwrap();
        assert!(limiter.did_acquire(45).unwrap());
        assert!(!limiter.did_acquire(1).unwrap());
    }
}
