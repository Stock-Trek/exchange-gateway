use std::time::Instant;

#[derive(Debug, Clone)]
pub(crate) struct RateLimiterState {
    interval_nanos: u128,
    capacity_per_interval: u32,
    current_capacity: u32,
    last_calculation: Instant,
    excess_interval_nanos: u128,
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
        }
    }
    #[must_use]
    pub fn did_consume(&mut self, cost: u32) -> bool {
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
