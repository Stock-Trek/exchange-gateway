use exchange_types::{
    new_types::UsageCount,
    rate_limited::{RateLimit, RateLimits},
};
use std::collections::HashMap;

pub(crate) struct UnlimitedRateLimits;

impl RateLimits for UnlimitedRateLimits {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        HashMap::new()
    }
}
