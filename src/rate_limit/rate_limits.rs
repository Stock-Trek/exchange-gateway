use crate::{
    error::{EGError, EGResult},
    rate_limit::{feedback::RateLimitFeedback, rate_limiter::RateLimiter},
};

/// The rate limits enforced locally by a connector.
///
/// Binance enforces independent limits for request weight (shared by all
/// request types, `REQUEST_WEIGHT` 6000/min per IP) and order count
/// (`ORDERS` 50 per 10 seconds and 160000 per day per account). Modelling
/// them as separate buckets means a burst of `exchangeInfo` polls no longer
/// eats into the order budget, and order placement is throttled at Binance's
/// actual rate rather than an approximation of it.
#[derive(Debug, Clone)]
pub(crate) struct RateLimits {
    pub weight: RateLimiter,
    pub orders: RateLimiter,
}

impl RateLimits {
    pub fn refund(&self, weight: u32, orders: u32) -> EGResult<()> {
        self.weight.refund(weight)?;
        self.orders.refund(orders)
    }
    /// Applies server-side rate-limit feedback to the local model.
    ///
    /// A 429/418 response throttles every bucket until `Retry-After` elapses
    /// (the IP is throttled or banned, so neither weight nor orders can be
    /// sent). Reported usage then realigns each bucket's remaining capacity
    /// and limit with what the server actually enforces, so hard-coded weights
    /// (e.g. `exchangeInfo`, which is dynamic on Binance) cannot drift
    /// undetected. `exchangeInfo` feedback carries the limit definitions
    /// without usage, so it adopts the limits without resetting locally
    /// consumed capacity.
    pub fn apply_feedback(&self, feedback: &RateLimitFeedback) -> EGResult<()> {
        if feedback.throttled || feedback.retry_after.is_some() {
            self.weight.throttle(feedback.retry_after)?;
            self.orders.throttle(feedback.retry_after)?;
        }
        for usage in &feedback.usage {
            self.weight.apply_usage(usage)?;
            self.orders.apply_usage(usage)?;
        }
        Ok(())
    }
    /// Applies feedback carried by a rejected request's error, if any.
    ///
    /// A server-rejected 429/418 travels back as [`EGError::RateLimited`]
    /// carrying the throttling + usage feedback observed on the response, so
    /// the local model is realigned even though the request failed. Local
    /// rejections carry no feedback and are a no-op here.
    pub fn apply_feedback_from_error(&self, error: &EGError) -> EGResult<()> {
        if let EGError::RateLimited { feedback } = error {
            self.apply_feedback(feedback)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::feedback::RateLimitUsage;
    use std::time::Duration;

    #[test]
    fn throttled_feedback_drains_buckets_until_retry_after() {
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 50,
                    interval_nanos: Duration::from_secs(10).as_nanos(),
                },
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 160_000,
                    interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                },
            ]),
        };
        assert!(limits.weight.did_acquire(1).unwrap());
        assert!(limits.orders.did_acquire(1).unwrap());
        limits
            .apply_feedback(&RateLimitFeedback {
                throttled: true,
                retry_after: Some(Duration::from_secs(30)),
                usage: vec![RateLimitUsage {
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                    used: Some(6000),
                    limit: None,
                }],
            })
            .unwrap();
        assert!(!limits.weight.did_acquire(1).unwrap());
        assert!(!limits.orders.did_acquire(1).unwrap());
    }

    #[test]
    fn usage_feedback_realigns_bucket_capacity() {
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 50,
                    interval_nanos: Duration::from_secs(10).as_nanos(),
                },
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 160_000,
                    interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                },
            ]),
        };
        // The exchangeInfo response reports a lowered request-weight limit of
        // 4000 with 3000 already used: the bucket must adopt both.
        limits
            .apply_feedback(&RateLimitFeedback {
                throttled: false,
                retry_after: None,
                usage: vec![RateLimitUsage {
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                    used: Some(3000),
                    limit: Some(4000),
                }],
            })
            .unwrap();
        assert!(limits.weight.did_acquire(1000).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[test]
    fn exchange_info_limit_only_feedback_keeps_local_consumption() {
        // Regression test: REST exchangeInfo rateLimits entries carry the
        // limit definitions but never a usage count, so applying them must
        // adopt the limits without refilling the buckets to `limit - 0`.
        // Otherwise every exchangeInfo poll would wipe out all locally
        // consumed capacity and disable local rate limiting.
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 50,
                    interval_nanos: Duration::from_secs(10).as_nanos(),
                },
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 160_000,
                    interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                },
            ]),
        };
        assert!(limits.weight.did_acquire(1000).unwrap());
        assert!(limits.orders.did_acquire(10).unwrap());
        limits
            .apply_feedback(&RateLimitFeedback {
                throttled: false,
                retry_after: None,
                usage: vec![
                    RateLimitUsage {
                        interval_nanos: Duration::from_secs(60).as_nanos(),
                        used: None,
                        limit: Some(6000),
                    },
                    RateLimitUsage {
                        interval_nanos: Duration::from_secs(10).as_nanos(),
                        used: None,
                        limit: Some(50),
                    },
                    RateLimitUsage {
                        interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                        used: None,
                        limit: Some(160_000),
                    },
                ],
            })
            .unwrap();
        // The weight bucket still has 6000 - 1000 = 5000 left, not a full
        // 6000, and the 10s order bucket 50 - 10 = 40 left, not a full 50.
        assert!(limits.weight.did_acquire(5000).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
        assert!(limits.orders.did_acquire(40).unwrap());
        assert!(!limits.orders.did_acquire(1).unwrap());
    }

    #[test]
    fn rejected_request_error_applies_server_feedback() {
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![]),
        };
        // A 429 rejection travels back as RateLimited carrying the server's
        // feedback; applying it must drain the buckets until Retry-After.
        limits
            .apply_feedback_from_error(&crate::error::EGError::RateLimited {
                feedback: RateLimitFeedback {
                    throttled: true,
                    retry_after: Some(Duration::from_secs(30)),
                    usage: vec![],
                },
            })
            .unwrap();
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[test]
    fn rejected_request_error_without_feedback_is_noop() {
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![]),
        };
        limits
            .apply_feedback_from_error(&crate::error::EGError::BadResponse)
            .unwrap();
        assert!(limits.weight.did_acquire(1).unwrap());
    }
}
