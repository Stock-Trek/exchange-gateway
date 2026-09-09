use exchange_types::{
    new_types::UsageCount,
    rate_limited::{RateLimit, RateLimits},
};
use std::collections::HashMap;

pub(crate) struct NoRateLimits;

impl RateLimits for NoRateLimits {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        HashMap::new()
    }
}
