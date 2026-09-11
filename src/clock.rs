use crate::error::{EGError, EGResult};
use exchange_types::new_types::Milliseconds;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub struct Clock {
    server_offset_millis: AtomicI64,
    last_sync: Mutex<Option<Instant>>,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}

impl Clock {
    pub fn new() -> Self {
        Self {
            server_offset_millis: AtomicI64::new(0),
            last_sync: Mutex::new(None),
        }
    }

    pub fn duration_since_last_sync(&self) -> EGResult<Option<Duration>> {
        let result = self.last_sync.lock();
        let mutex = result.map_err(|_| EGError::MutexPoisoned)?;
        let last_sync = *mutex;
        Ok(last_sync.map(|i| i.elapsed()))
    }

    pub fn is_synced(&self) -> EGResult<bool> {
        let last_sync = self.last_sync.lock().map_err(|_| EGError::MutexPoisoned)?;
        Ok(last_sync.is_some())
    }
    pub fn sync(&self, server_time: Milliseconds, round_trip_duration: Duration) -> EGResult<()> {
        let system_time = Self::system_time()?;
        let round_trip_time = Milliseconds(round_trip_duration.as_millis() as i64);
        let system_time_estimate = system_time - (round_trip_time / 2);
        let offset_estimate = system_time_estimate - server_time;
        self.server_offset_millis
            .store(offset_estimate.0, Ordering::Relaxed);
        let mut last_sync = self.last_sync.lock().map_err(|_| EGError::MutexPoisoned)?;
        *last_sync = Some(Instant::now());
        Ok(())
    }
    pub fn server_time_estimate(&self) -> EGResult<Milliseconds> {
        if !self.is_synced()? {
            return Err(EGError::ClockNotSynced);
        }
        self.server_time_estimate_unchecked()
    }
    pub fn server_time_estimate_unchecked(&self) -> EGResult<Milliseconds> {
        Ok(Self::system_time()? - self.server_offset())
    }

    fn system_time() -> EGResult<Milliseconds> {
        let system_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| EGError::SystemTimeBeforeUnixEpoch)?;
        Ok(Milliseconds(system_time.as_millis() as i64))
    }
    fn server_offset(&self) -> Milliseconds {
        Milliseconds(self.server_offset_millis.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_time_estimate_errors_before_sync() {
        let clock = Clock::new();
        assert!(!clock.is_synced().unwrap());
        assert!(matches!(
            clock.server_time_estimate(),
            Err(EGError::ClockNotSynced)
        ));
    }

    #[test]
    fn server_time_estimate_succeeds_after_sync() {
        let clock = Clock::new();
        let server_time = Milliseconds(1_700_000_000_000);
        clock.sync(server_time, Duration::from_millis(20)).unwrap();
        assert!(clock.is_synced().unwrap());
        let estimate = clock.server_time_estimate().unwrap();
        let difference = (estimate.0 - server_time.0).abs();
        assert!(
            difference < 1_000,
            "estimate {estimate:?} too far from server time"
        );
    }
}
