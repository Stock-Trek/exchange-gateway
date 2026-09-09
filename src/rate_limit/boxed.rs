use exchange_types::{
    new_types::UsageCount,
    rate_limited::{RateLimit, RateLimits},
};
use std::collections::HashMap;

pub(crate) struct BoxedRateLimits(pub(crate) Box<dyn RateLimits>);

impl RateLimits for BoxedRateLimits {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        self.0.default_capacity()
    }
}
