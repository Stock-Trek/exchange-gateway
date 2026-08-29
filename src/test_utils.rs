//! Test-only helpers shared across modules. This module is only compiled
//! into test builds, so nothing here ships in the library.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

/// A stand-in for the system clock that tests advance manually, so
/// time-dependent behaviour (rate-limiter refill windows, throttle
/// deadlines) can be exercised deterministically instead of sleeping.
pub(crate) struct TestClock {
    base: Instant,
    offset: AtomicU64,
}

impl TestClock {
    pub(crate) fn new() -> Self {
        Self {
            base: Instant::now(),
            offset: AtomicU64::new(0),
        }
    }

    /// The current reading of this clock.
    pub(crate) fn instant(&self) -> Instant {
        self.base + Duration::from_nanos(self.offset.load(Ordering::Relaxed))
    }

    /// Advances the clock by `duration`.
    pub(crate) fn advance(&self, duration: Duration) {
        self.offset
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// A `now` readout tied to this clock, for injecting into code that
    /// reads the time through a function (e.g. the rate limiter).
    pub(crate) fn now_fn(self: &Arc<Self>) -> Arc<dyn Fn() -> Instant + Send + Sync> {
        let clock = Arc::clone(self);
        Arc::new(move || clock.instant())
    }
}
