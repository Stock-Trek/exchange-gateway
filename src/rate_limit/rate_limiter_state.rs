use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct RateLimiterState {
    interval_nanos: u128,
    capacity_per_interval: u32,
    current_capacity: u32,
    last_calculation: Instant,
    excess_interval_nanos: u128,
    throttled_until: Option<Instant>,
}

impl RateLimiterState {
    pub fn new(interval_nanos: u128, capacity_per_interval: u32) -> Self {
        assert!(interval_nanos > 0, "interval_nanos cannot be zero");
        assert!(
            capacity_per_interval > 0,
            "capacity_per_interval cannot be zero"
        );
        Self {
            interval_nanos,
            capacity_per_interval,
            current_capacity: capacity_per_interval,
            last_calculation: Instant::now(),
            excess_interval_nanos: 0,
            throttled_until: None,
        }
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
    /// Drops the bucket to zero and refuses to refill it until `until` elapses.
    ///
    /// Used when the server answers 429/418 with a `Retry-After` header: the
    /// local model must not keep admitting requests while the server is
    /// throttling (or banning) our IP.
    pub fn throttle(&mut self, until: Instant) {
        self.current_capacity = 0;
        self.throttled_until = Some(until);
    }
    /// Realigns the bucket with server-reported usage.
    ///
    /// `used`/`limit` come from the exchange (usage headers, WebSocket
    /// `rateLimits`, `exchangeInfo`), so the bucket tracks the server even
    /// when the locally hard-coded weight or limit has drifted. The refill
    /// window restarts from now. An absent limit keeps the configured limit
    /// and only trims remaining capacity down to `limit - used`.
    pub fn sync_usage(&mut self, used: u32, limit: Option<u32>) {
        if let Some(limit) = limit {
            self.capacity_per_interval = limit;
            self.current_capacity = self.capacity_per_interval.saturating_sub(used.min(limit));
        } else {
            let remaining = self
                .capacity_per_interval
                .saturating_sub(used.min(self.capacity_per_interval));
            self.current_capacity = self.current_capacity.min(remaining);
        }
        self.last_calculation = Instant::now();
        self.excess_interval_nanos = 0;
    }

    fn is_throttled(&self) -> bool {
        self.throttled_until
            .is_some_and(|throttled_until| Instant::now() < throttled_until)
    }
    fn did_quick_consume(&mut self, cost: u32) -> bool {
        // A single request can never consume more than the full bucket
        // capacity. Refuse rather than panic: a request weight that exceeds
        // the configured capacity must not take down the whole process.
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
                // Still throttled: keep the bucket empty and do not refill.
                return;
            }
            self.throttled_until = None;
            // Refill starts counting once the throttle has elapsed.
            self.last_calculation = throttled_until;
        }
        let elapsed_nanos = now.duration_since(self.last_calculation).as_nanos();
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
        let mut state = RateLimiterState::new(Duration::from_secs(60).as_nanos(), 10);
        assert!(state.did_consume(1));
        state.throttle(Instant::now() + Duration::from_secs(60));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn throttle_expires_and_refills_from_deadline() {
        let mut state = RateLimiterState::new(Duration::from_millis(10).as_nanos(), 10);
        state.throttle(Instant::now() + Duration::from_millis(20));
        std::thread::sleep(Duration::from_millis(30));
        // Bucket refills from the throttle deadline, so capacity returns.
        assert!(state.did_consume(10));
    }

    #[test]
    fn sync_usage_realigns_capacity_and_limit() {
        let mut state = RateLimiterState::new(Duration::from_secs(60).as_nanos(), 6000);
        let _ = state.did_consume(5000);
        // Server reports 3000 used out of a newly lowered limit of 4000.
        state.sync_usage(3000, Some(4000));
        assert!(state.did_consume(1000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_without_limit_only_trims_capacity() {
        let mut state = RateLimiterState::new(Duration::from_secs(60).as_nanos(), 6000);
        let _ = state.did_consume(3000);
        // Server reports 5500 used in the last minute but no limit: remaining
        // capacity is trimmed to 500, never increased.
        state.sync_usage(5500, None);
        assert!(state.did_consume(500));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_without_limit_never_adds_capacity() {
        let mut state = RateLimiterState::new(Duration::from_secs(60).as_nanos(), 6000);
        let _ = state.did_consume(1000);
        // Server reports low usage: the bucket must not be refilled beyond
        // what the local model has already accounted for.
        state.sync_usage(100, None);
        assert!(state.did_consume(5000));
        assert!(!state.did_consume(1));
    }

    #[test]
    fn sync_usage_keeps_throttle() {
        let mut state = RateLimiterState::new(Duration::from_secs(60).as_nanos(), 10);
        state.throttle(Instant::now() + Duration::from_secs(60));
        state.sync_usage(0, Some(10));
        assert!(!state.did_consume(10));
    }

    #[test]
    fn sync_usage_with_usage_above_limit_drains_bucket() {
        let mut state = RateLimiterState::new(Duration::from_secs(60).as_nanos(), 10);
        state.sync_usage(20, Some(10));
        assert!(!state.did_consume(1));
    }
}
