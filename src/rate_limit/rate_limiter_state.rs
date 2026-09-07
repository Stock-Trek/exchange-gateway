use exchange_types::rate_limited::RateLimitRestriction;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Clone)]
pub(crate) struct RateLimiterState {
    restriction: RateLimitRestriction,
    interval_nanos: u128,
    capacity_per_interval: u32,
    current_capacity: u32,
    last_calculation: Instant,
    excess_interval_nanos: u128,
    throttled_until: Option<Instant>,
    // The wall clock by default, but injectable so tests can advance time
    // deterministically instead of sleeping.
    now: Arc<dyn Fn() -> Instant + Send + Sync>,
}

impl RateLimiterState {
    pub fn new(
        rate_limit_type: RateLimitRestriction,
        interval_nanos: u128,
        capacity_per_interval: u32,
    ) -> Self {
        Self::with_clock(
            rate_limit_type,
            interval_nanos,
            capacity_per_interval,
            Arc::new(Instant::now),
        )
    }
    /// Builds a state that reads time from `now` instead of the wall clock,
    /// so time-based refill behaviour can be exercised without sleeping.
    pub(crate) fn with_clock(
        restriction: RateLimitRestriction,
        interval_nanos: u128,
        capacity_per_interval: u32,
        now: Arc<dyn Fn() -> Instant + Send + Sync>,
    ) -> Self {
        assert!(interval_nanos > 0, "interval_nanos cannot be zero");
        assert!(
            capacity_per_interval > 0,
            "capacity_per_interval cannot be zero"
        );
        Self {
            restriction,
            interval_nanos,
            capacity_per_interval,
            current_capacity: capacity_per_interval,
            last_calculation: now(),
            excess_interval_nanos: 0,
            throttled_until: None,
            now,
        }
    }
    pub fn restriction(&self) -> RateLimitRestriction {
        self.restriction
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
    /// Throttles until `self.now() + duration`, so the deadline is measured
    /// on the same clock the state reads time from.
    pub(crate) fn throttle_after(&mut self, duration: Duration) {
        self.throttle(self.now() + duration);
    }
    pub fn sync_usage(&mut self, used: Option<u32>, limit: Option<u32>) {
        if let Some(limit) = limit {
            self.capacity_per_interval = limit;
        }
        match (used, limit) {
            (Some(used), Some(limit)) => {
                if self.is_throttled() {
                    // Still inside the Retry-After window: keep the bucket
                    // empty so it refills from the deadline instead of
                    // instantly granting limit - used.
                    self.current_capacity = 0;
                } else {
                    // The deadline has passed (or there was none): realign to
                    // the server-reported usage. Clear the stale deadline so
                    // the next refill doesn't bank capacity from before the
                    // feedback arrived.
                    self.throttled_until = None;
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
        self.last_calculation = self.now();
        self.excess_interval_nanos = 0;
    }

    fn now(&self) -> Instant {
        (self.now)()
    }
    fn is_throttled(&self) -> bool {
        self.throttled_until
            .is_some_and(|throttled_until| self.now() < throttled_until)
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
        let now = self.now();
        if let Some(throttled_until) = self.throttled_until {
            if now < throttled_until {
                return;
            }
            self.throttled_until = None;
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

impl std::fmt::Debug for RateLimiterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiterState")
            .field("rate_limit_type", &self.restriction)
            .field("interval_nanos", &self.interval_nanos)
            .field("capacity_per_interval", &self.capacity_per_interval)
            .field("current_capacity", &self.current_capacity)
            .field("last_calculation", &self.last_calculation)
            .field("excess_interval_nanos", &self.excess_interval_nanos)
            .field("throttled_until", &self.throttled_until)
            .finish_non_exhaustive()
    }
}
