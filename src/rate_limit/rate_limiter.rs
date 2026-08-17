use crate::rate_limit::{rate_limit_config::RateLimitConfig, rate_limiter_state::RateLimiterState};
use std::sync::{Arc, Mutex};

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
    #[must_use]
    pub fn did_acquire(&self, cost: u32) -> bool {
        let mut limiters_guard = self.rate_limiters.lock().unwrap();
        for (index, limiter) in limiters_guard.iter_mut().enumerate() {
            if !limiter.did_consume(cost) {
                for i in 0..index {
                    let limiter = &mut limiters_guard[i];
                    limiter.refund(cost);
                }
                return false;
            }
        }
        true
    }
}
