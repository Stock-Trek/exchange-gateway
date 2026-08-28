use crate::{
    error::{EGError, EGResult},
    rate_limit::{feedback::RateLimitFeedback, rate_limiter::RateLimiter},
};

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
    pub fn apply_feedback(&self, feedback: &RateLimitFeedback) -> EGResult<()> {
        if feedback.is_throttled || feedback.retry_after.is_some() {
            self.weight.throttle(feedback.retry_after)?;
            self.orders.throttle(feedback.retry_after)?;
        }
        for usage in &feedback.usage {
            self.weight.apply_usage(usage)?;
            self.orders.apply_usage(usage)?;
        }
        Ok(())
    }
    pub fn apply_feedback_from_error(&self, error: &EGError) -> EGResult<()> {
        if let EGError::RateLimited(feedback) = error {
            self.apply_feedback(feedback)?;
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
    fn throttled_feedback_drains_buckets_until_retry_after() {
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::RequestWeight,
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::Orders,
                    capacity_per_interval: 50,
                    interval_nanos: Duration::from_secs(10).as_nanos(),
                },
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::Orders,
                    capacity_per_interval: 160_000,
                    interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                },
            ]),
        };
        assert!(limits.weight.did_acquire(1).unwrap());
        assert!(limits.orders.did_acquire(1).unwrap());
        limits
            .apply_feedback(&RateLimitFeedback {
                is_throttled: true,
                retry_after: Some(Duration::from_secs(30)),
                usage: vec![RateLimitUsage {
                    rate_limit_type: RateLimitType::RequestWeight,
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
                    rate_limit_type: RateLimitType::RequestWeight,
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::Orders,
                    capacity_per_interval: 50,
                    interval_nanos: Duration::from_secs(10).as_nanos(),
                },
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::Orders,
                    capacity_per_interval: 160_000,
                    interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
                },
            ]),
        };
        // The exchangeInfo response reports a lowered request-weight limit of
        // 4000 with 3000 already used: the bucket must adopt both.
        limits
            .apply_feedback(&RateLimitFeedback {
                is_throttled: false,
                retry_after: None,
                usage: vec![RateLimitUsage {
                    rate_limit_type: RateLimitType::RequestWeight,
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
            weight: RateLimiter::new(vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 6000,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }]),
            orders: RateLimiter::new(vec![
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
            ]),
        };
        assert!(limits.weight.did_acquire(1000).unwrap());
        assert!(limits.orders.did_acquire(10).unwrap());
        limits
            .apply_feedback(&RateLimitFeedback {
                is_throttled: false,
                retry_after: None,
                usage: vec![
                    RateLimitUsage {
                        rate_limit_type: RateLimitType::Orders,
                        interval_nanos: Duration::from_secs(60).as_nanos(),
                        used: None,
                        limit: Some(6000),
                    },
                    RateLimitUsage {
                        rate_limit_type: RateLimitType::Orders,
                        interval_nanos: Duration::from_secs(10).as_nanos(),
                        used: None,
                        limit: Some(50),
                    },
                    RateLimitUsage {
                        rate_limit_type: RateLimitType::Orders,
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
                    rate_limit_type: RateLimitType::RequestWeight,
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![]),
        };
        // A 429 rejection travels back as RateLimited carrying the server's
        // feedback; applying it must drain the buckets until Retry-After.
        limits
            .apply_feedback_from_error(&crate::error::EGError::RateLimited(RateLimitFeedback {
                is_throttled: true,
                retry_after: Some(Duration::from_secs(30)),
                usage: vec![],
            }))
            .unwrap();
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[test]
    fn rejected_request_error_without_feedback_is_noop() {
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::RequestWeight,
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

    #[test]
    fn raw_requests_usage_does_not_overwrite_weight_bucket() {
        // Binance reports REQUEST_WEIGHT (6000/min) and RAW_REQUESTS
        // (61000/min) on the same one-minute window, RAW_REQUESTS last. The
        // weight bucket must keep its own limit instead of adopting the
        // raw-requests one, or the client would send ~10x the server's real
        // weight limit.
        let limits = RateLimits {
            weight: RateLimiter::new(vec![
                crate::rate_limit::rate_limit_config::RateLimitConfig {
                    rate_limit_type: RateLimitType::RequestWeight,
                    capacity_per_interval: 6000,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                },
            ]),
            orders: RateLimiter::new(vec![]),
        };
        limits
            .apply_feedback(&RateLimitFeedback {
                is_throttled: false,
                retry_after: None,
                usage: vec![
                    RateLimitUsage {
                        rate_limit_type: RateLimitType::RequestWeight,
                        interval_nanos: Duration::from_secs(60).as_nanos(),
                        used: Some(3000),
                        limit: Some(6000),
                    },
                    RateLimitUsage {
                        rate_limit_type: RateLimitType::RawRequests,
                        interval_nanos: Duration::from_secs(60).as_nanos(),
                        used: Some(40_000),
                        limit: Some(61_000),
                    },
                ],
            })
            .unwrap();
        // 3000 remaining on the weight bucket; the raw-requests usage (and
        // its 61000 limit) must not have been applied to it.
        assert!(limits.weight.did_acquire(3000).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
    }
}
