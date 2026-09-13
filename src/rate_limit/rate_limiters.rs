use crate::{
    error::{EGError, EGResult},
    rate_limit::rate_limiter::RateLimiter,
};
use exchange_types::{
    new_types::{Nanoseconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction, RateUsage},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use strum::IntoEnumIterator;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct RateLimiters {
    limiters: HashMap<RateLimitRestriction, RateLimiter>,
    acquisition_lock: Arc<Mutex<()>>,
}

impl RateLimiters {
    pub fn new(mut limiters: HashMap<RateLimitRestriction, RateLimiter>) -> Self {
        for restriction in RateLimitRestriction::iter() {
            limiters
                .entry(restriction)
                .or_insert_with(|| RateLimiter::new(&[]));
        }
        Self {
            limiters,
            acquisition_lock: Arc::new(Mutex::new(())),
        }
    }
    pub fn remaining_capacity(&self) -> EGResult<HashMap<RateLimit, UsageCount>> {
        let mut capacities = HashMap::new();
        for (restriction, limiter) in &self.limiters {
            for (interval_nanos, remaining) in limiter.remaining_capacity()? {
                capacities.insert(
                    RateLimit {
                        restriction: *restriction,
                        interval_nanos,
                    },
                    remaining,
                );
            }
        }
        Ok(capacities)
    }
    pub fn did_acquire(&self, costs: &[(RateLimitRestriction, UsageCount)]) -> EGResult<()> {
        let _guard = self
            .acquisition_lock
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?;
        for limiter in self.limiters.values() {
            if limiter.is_throttled()? {
                debug!("rate limiter is throttled, rejecting request");
                return Err(EGError::RateLimited);
            }
        }
        let mut acquired = Vec::with_capacity(costs.len());
        for &(restriction, cost) in costs {
            if let Some(limiter) = self.limiters.get(&restriction) {
                if let Err(error) = limiter.did_acquire(cost) {
                    self.refund_acquired(&acquired)?;
                    return Err(error);
                }
                acquired.push((restriction, cost));
            }
        }
        Ok(())
    }
    fn refund_acquired(&self, acquired: &[(RateLimitRestriction, UsageCount)]) -> EGResult<()> {
        for &(restriction, cost) in acquired {
            self.refund(restriction, cost)?;
        }
        Ok(())
    }
    pub fn refund(&self, restriction: RateLimitRestriction, cost: UsageCount) -> EGResult<()> {
        if let Some(limiter) = self.limiters.get(&restriction) {
            limiter.refund(cost)
        } else {
            Ok(())
        }
    }
    pub fn set_usage(&self, usage: &HashMap<RateLimit, RateUsage>) -> EGResult<()> {
        for (restriction, limiter) in &self.limiters {
            let restriction_usage = usage
                .iter()
                .filter(|(rate_limit, _)| rate_limit.restriction == *restriction)
                .map(|(rate_limit, rate_usage)| (rate_limit.interval_nanos, *rate_usage))
                .collect::<HashMap<Nanoseconds, RateUsage>>();
            limiter.set_usage(&restriction_usage)?;
        }
        Ok(())
    }
    pub fn set_retry_after(&self, retry_after: Duration) -> EGResult<()> {
        warn!(
            ?retry_after,
            "exchange requested Retry-After, throttling requests"
        );
        for limiter in self.limiters.values() {
            limiter.throttle(retry_after)?;
        }
        Ok(())
    }
}

pub(crate) struct RateLimitGuard<'a> {
    rate_limiters: &'a RateLimiters,
    costs: Vec<(RateLimitRestriction, UsageCount)>,
    armed: bool,
}

impl<'a> RateLimitGuard<'a> {
    pub(crate) fn new(
        rate_limiters: &'a RateLimiters,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> Self {
        Self {
            rate_limiters,
            costs,
            armed: true,
        }
    }
    pub(crate) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for RateLimitGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            for &(restriction, cost) in &self.costs {
                let _ = self.rate_limiters.refund(restriction, cost);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::rate_limiter_state::RateLimiterState;

    fn rate_limiters() -> RateLimiters {
        let state = RateLimiterState::try_new(Nanoseconds(60_000_000_000), UsageCount(10))
            .expect("valid rate limiter state");
        RateLimiters::new(HashMap::from([(
            RateLimitRestriction::RawRequests,
            RateLimiter::new(&[state]),
        )]))
    }

    #[test]
    fn retry_after_blocks_zero_cost_requests() {
        let rate_limiters = rate_limiters();
        rate_limiters
            .set_retry_after(Duration::from_secs(60))
            .expect("set retry after");
        assert!(matches!(
            rate_limiters.did_acquire(&[]),
            Err(EGError::RateLimited)
        ));
    }

    #[test]
    fn zero_cost_requests_are_allowed_without_retry_after() {
        let rate_limiters = rate_limiters();
        assert!(rate_limiters.did_acquire(&[]).is_ok());
    }

    fn remaining(rate_limiters: &RateLimiters) -> UsageCount {
        rate_limiters
            .remaining_capacity()
            .expect("remaining capacity")
            .into_iter()
            .find(|(rate_limit, _)| rate_limit.restriction == RateLimitRestriction::RawRequests)
            .map(|(_, remaining)| remaining)
            .expect("raw requests restriction present")
    }

    #[test]
    fn guard_refunds_acquired_cost_on_drop() {
        let rate_limiters = rate_limiters();
        let costs = vec![(RateLimitRestriction::RawRequests, UsageCount(3))];
        rate_limiters.did_acquire(&costs).expect("acquire");
        assert_eq!(remaining(&rate_limiters), UsageCount(7));

        {
            let _guard = RateLimitGuard::new(&rate_limiters, costs);
        }

        assert_eq!(remaining(&rate_limiters), UsageCount(10));
    }

    #[test]
    fn disarmed_guard_keeps_acquired_cost() {
        let rate_limiters = rate_limiters();
        let costs = vec![(RateLimitRestriction::RawRequests, UsageCount(3))];
        rate_limiters.did_acquire(&costs).expect("acquire");

        RateLimitGuard::new(&rate_limiters, costs).disarm();

        assert_eq!(remaining(&rate_limiters), UsageCount(7));
    }
}
