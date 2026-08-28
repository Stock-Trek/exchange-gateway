use crate::rate_limit::rate_limit_type::RateLimitType;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitUsage {
    pub rate_limit_type: RateLimitType,
    pub interval_nanos: u128,
    pub used: Option<u32>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RateLimitFeedback {
    pub usage: Vec<RateLimitUsage>,
    pub retry_after: Option<Duration>,
    pub is_throttled: bool,
}

impl RateLimitFeedback {
    pub(crate) fn has_retry_feedback(&self) -> bool {
        self.is_throttled || self.retry_after.is_some()
    }
}
