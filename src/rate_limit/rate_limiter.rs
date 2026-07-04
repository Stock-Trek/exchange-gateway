use crate::rate_limit::{rate_limit_config::RateLimitConfig, rate_limiter_state::RateLimiterState};
use std::sync::{Arc, Mutex};

#[allow(unused)]
#[derive(Debug, Clone)]
pub(crate) struct RateLimiter {
    state: Arc<Mutex<RateLimiterState>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

#[allow(unused)]
impl RateLimiter {
    pub fn new(rate_limit: RateLimitConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(rate_limit.to_state())),
        }
    }
    #[must_use]
    pub async fn did_acquire(&self, cost: u32) -> bool {
        let mut state_guard = self.state.lock().unwrap();
        state_guard.did_consume(cost)
    }
}
