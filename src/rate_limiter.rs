use exchange_types::rate_limited::RateLimitType;
use std::fmt::Debug;

pub trait RateLimiter: Debug + Send + Sync {
    fn did_acquire(&self, limit_costs: &Vec<(RateLimitType, u32)>) -> bool;
}
