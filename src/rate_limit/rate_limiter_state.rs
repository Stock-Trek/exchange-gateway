use crate::rate_limit::rate_limit_type::RateLimitType;
use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct RateLimiterState {
    rate_limit_type: RateLimitType,
    interval_nanos: u128,
    capacity_per_interval: u32,
    current_capacity: u32,
    last_calculation: Instant,
    excess_interval_nanos: u128,
    throttled_until: Option<Instant>,
}

impl RateLimiterState {
    pub fn new(
        rate_limit_type: RateLimitType,
        interval_nanos: u128,
        capacity_per_interval: u32,
    ) -> Self {
        assert!(interval_nanos > 0, "interval_nanos cannot be zero");
        assert!(
            capacity_per_interval > 0,
            "capacity_per_interval cannot be zero"
        );
        Self {
            rate_limit_type,
            interval_nanos,
            capacity_per_interval,
            current_capacity: capacity_per_interval,
            last_calculation: Instant::now(),
            excess_interval_nanos: 0,
            throttled_until: None,
        }
    }
    pub fn rate_limit_type(&self) -> RateLimitType {
        self.rate_limit_type
    }
    pub fn interval_nanos(&self) -> u128 {
        self.interval_nanos
    }
    #[must_use]
    pub fn did_consume(&mut self, cost: u32) -> bool {
        if self.is_throttled() {
            return false;
        }
        if self.did_quick_consume(cost) {
            true
        } else {
            self.update_capacity();
            self.did_quick_consume(cost)
        }
    }
    pub fn refund(&mut self, cost: u32) {
        self.current_capacity = (self.current_capacity + cost).min(self.capacity_per_interval);
    }
    pub fn throttle(&mut self, until: Instant) {
        self.current_capacity = 0;
        self.throttled_until = Some(until);
    }
    pub fn sync_usage(&mut self, used: Option<u32>, limit: Option<u32>) {
        if let Some(limit) = limit {
            self.capacity_per_interval = limit;
        }
        match (used, limit) {
            (Some(used), Some(limit)) => {
                if self.throttled_until.is_some() {
                    self.current_capacity = 0;
                } else {
                    self.current_capacity =
                        self.capacity_per_interval.saturating_sub(used.min(limit));
                }
            }
            (Some(used), None) => {
                let remaining = self
                    .capacity_per_interval
                    .saturating_sub(used.min(self.capacity_per_interval));
                self.current_capacity = self.current_capacity.min(remaining);
            }
            (None, Some(limit)) => {
                self.current_capacity = self.current_capacity.min(limit);
            }
            (None, None) => {}
        }
        self.last_calculation = Instant::now();
        self.excess_interval_nanos = 0;
    }

    fn is_throttled(&self) -> bool {
        self.throttled_until
            .is_some_and(|throttled_until| Instant::now() < throttled_until)
    }
    fn did_quick_consume(&mut self, cost: u32) -> bool {
        if cost > self.capacity_per_interval {
            return false;
        }
        let consumed = self.current_capacity >= cost;
        if consumed {
            self.current_capacity -= cost;
        }
        consumed
    }
    fn update_capacity(&mut self) {
        let now = Instant::now();
        if let Some(throttled_until) = self.throttled_until {
            if now < throttled_until {
                return;
            }
            self.throttled_until = None;
            self.last_calculation = throttled_until;
        }
        let elapsed_nanos = (now - self.last_calculation).as_nanos();
        let total_nanos = self.excess_interval_nanos + elapsed_nanos;
        let complete_intervals = total_nanos / self.interval_nanos;
        if complete_intervals > 0 {
            let capacity_per_interval = self.capacity_per_interval as u64;
            let capacity_to_add = complete_intervals as u64 * capacity_per_interval;
            let capacity_potentially_over_max = self.current_capacity as u64 + capacity_to_add;
            let limited_capacity = capacity_potentially_over_max.min(capacity_per_interval);
            self.current_capacity = limited_capacity as u32;
            self.excess_interval_nanos = total_nanos % self.interval_nanos;
            self.last_calculation = now;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn throttle_empties_bucket_until_deadline() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            10,
        );
        assert!(state.did_consume(1));
        state.throttle(Instant::now() + Duration::from_secs(60));
        assert!(!state.did_consume(1));
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_expires_and_refills_from_deadline() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_millis(10).as_nanos(),
            10,
        );
        state.throttle(Instant::now() + Duration::from_millis(20));
        tokio::time::advance(Duration::from_millis(30)).await;
        // Bucket refills from the throttle deadline, so capacity returns.
        assert!(state.did_consume(10));
    }

    #[test]
    fn sync_usage_realigns_capacity_and_limit() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(5000);
        // Server reports 3000 used out of a newly lowered limit of 4000.
        state.sync_usage(Some(3000), Some(4000));
        assert!(state.did_consume(1000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_without_limit_only_trims_capacity() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(3000);
        // Server reports 5500 used in the last minute but no limit: remaining
        // capacity is trimmed to 500, never increased.
        state.sync_usage(Some(5500), None);
        assert!(state.did_consume(500));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_without_limit_never_adds_capacity() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(1000);
        // Server reports low usage: the bucket must not be refilled beyond
        // what the local model has already accounted for.
        state.sync_usage(Some(100), None);
        assert!(state.did_consume(5000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_keeps_throttle() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            10,
        );
        state.throttle(Instant::now() + Duration::from_secs(60));
        state.sync_usage(Some(0), Some(10));
        assert!(!state.did_consume(10));
    }

    #[tokio::test(start_paused = true)]
    async fn sync_usage_with_limit_while_throttled_keeps_bucket_empty() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        state.throttle(Instant::now() + Duration::from_millis(20));
        // Limit-carrying usage arriving inside the throttle window (e.g. a
        // concurrent exchangeInfo response while a 429/Retry-After is active)
        // must not repopulate the bucket: it stays empty and refills from
        // zero after the deadline instead of instantly granting limit - used.
        state.sync_usage(Some(1200), Some(6000));
        tokio::time::advance(Duration::from_millis(30)).await;
        // Throttle elapsed, but the 60s refill window has barely started:
        // the bucket must not grant the full remaining quota at once.
        assert!(!state.did_consume(4800));
        assert!(!state.did_consume(1));
    }

    #[tokio::test(start_paused = true)]
    async fn sync_usage_with_limit_while_throttled_refills_after_deadline() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_millis(10).as_nanos(),
            6000,
        );
        state.throttle(Instant::now() + Duration::from_millis(20));
        state.sync_usage(Some(1200), Some(6000));
        tokio::time::advance(Duration::from_millis(50)).await;
        // The bucket refills from the throttle deadline up to the newly
        // reported limit rather than staying stuck at zero.
        assert!(state.did_consume(6000));
    }

    #[test]
    fn sync_usage_with_limit_only_adopts_limit_without_refilling() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(5000);
        // A limit definition without a usage count (REST `exchangeInfo`): the
        // new limit is adopted, but the bucket must not be refilled to
        // limit - 0 = limit — the 5000 locally-consumed capacity stays gone.
        state.sync_usage(None, Some(4000));
        assert!(state.did_consume(1000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_with_limit_only_never_adds_capacity() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(1000);
        // The server raises the limit; the bucket adopts it but must not gain
        // the difference as if the server had reported zero usage.
        state.sync_usage(None, Some(6000));
        assert!(state.did_consume(5000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_with_limit_only_trims_capacity_above_new_limit() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(1000);
        // The server lowers the limit below the remaining capacity: the
        // bucket is trimmed to the new limit.
        state.sync_usage(None, Some(2000));
        assert!(state.did_consume(2000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_without_usage_or_limit_is_noop() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            6000,
        );
        let _ = state.did_consume(1000);
        state.sync_usage(None, None);
        assert!(state.did_consume(5000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_with_usage_above_limit_drains_bucket() {
        let mut state = RateLimiterState::new(
            RateLimitType::RequestWeight,
            Duration::from_secs(60).as_nanos(),
            10,
        );
        state.sync_usage(Some(20), Some(10));
        assert!(!state.did_consume(1));
    }
}
