use crate::{error::EGResult, rate_limit::rate_limiter::RateLimiter};

#[derive(Debug, Clone)]
pub(crate) struct RateLimits {
    pub request: RateLimiter,
}

impl RateLimits {
    pub fn refund(&self, cost: u32) -> EGResult<()> {
        self.request.refund(cost)
    }
}
