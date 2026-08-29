use crate::{
    error::{EGError, EGResult},
    rate_limit::{
        feedback::RateLimitUsage, rate_limit_config::RateLimitConfig,
        rate_limiter_state::RateLimiterState,
    },
};
use std::{
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
    pub fn throttle(&self, retry_after: Option<Duration>) -> EGResult<()> {
        let mut limiters_guard = self
            .rate_limiters
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in limiters_guard.iter_mut() {
            limiter.throttle(retry_after);
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
    use std::time::{Duration, Instant};

    impl RateLimiter {
        /// Like [`Self::new`], but reading the clock through `now` so tests can
        /// drive time-dependent behaviour deterministically.
        pub(crate) fn new_with_clock(
            rate_limits: Vec<RateLimitConfig>,
            now: Arc<dyn Fn() -> Instant + Send + Sync>,
        ) -> Self {
            Self {
                rate_limiters: Arc::new(Mutex::new(
                    rate_limits
                        .iter()
                        .map(|rl| rl.to_state_with_clock(now.clone()))
                        .collect(),
                )),
            }
        }
    }

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
                used: Some(1000),
                limit: Some(6000),
            })
            .unwrap();
        limiter
            .apply_usage(&RateLimitUsage {
                rate_limit_type: RateLimitType::RawRequests,
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used: Some(60_000),
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
                used: Some(5000),
                limit: Some(10_000),
            })
            .unwrap();
        assert!(limiter.did_acquire(45).unwrap());
        assert!(!limiter.did_acquire(1).unwrap());
    }
}
