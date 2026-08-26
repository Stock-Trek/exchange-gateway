use crate::{error::EGResult, rate_limit::rate_limiter::RateLimiter};

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
}
