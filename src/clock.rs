use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, TryConvertRef},
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub struct Clock {
    offset_millis: AtomicI64,
    last_sync: Mutex<Option<Instant>>,
}

pub(crate) struct Synchronization<EGUnsignedReq, EGRes> {
    pub create_time_request: fn() -> (EGUnsignedReq, ArcPredicate<EGRes>),
    pub timeout: Duration,
    pub to_server_time: TryConvertRef<EGRes, i64>,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new()
    }
}

impl Clone for Clock {
    fn clone(&self) -> Self {
        let offset = self.offset_millis();
        let last_sync = *self
            .last_sync
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self {
            offset_millis: AtomicI64::from(offset),
            last_sync: Mutex::from(last_sync),
        }
    }
}

impl Clock {
    pub fn new() -> Self {
        Self {
            offset_millis: AtomicI64::new(0),
            last_sync: Mutex::new(None),
        }
    }

    pub fn offset_millis(&self) -> i64 {
        self.offset_millis.load(Ordering::Relaxed)
    }

    pub fn duration_since_last_sync(&self) -> EGResult<Duration> {
        let result = self.last_sync.lock();
        let mutex = result.map_err(|_| EGError::MutexPoisoned)?;
        let last_sync = *mutex;
        Ok(last_sync.map_or(Duration::MAX, |i| i.elapsed()))
    }

    pub fn sync(&self, server_time_millis: i64, round_trip_time: Duration) -> EGResult<()> {
        let rtt_ms = round_trip_time.as_millis() as i64;
        let system_millis = Self::system_millis();
        let midpoint_system_millis = system_millis - (rtt_ms / 2);
        let new_offset = midpoint_system_millis - server_time_millis;
        let mut last_sync = self.last_sync.lock().map_err(|_| EGError::MutexPoisoned)?;
        self.offset_millis.store(new_offset, Ordering::Relaxed);
        *last_sync = Some(Instant::now());
        Ok(())
    }

    pub fn now_millis(&self) -> i64 {
        let system_millis = Self::system_millis();
        let offset = self.offset_millis.load(Ordering::Relaxed);
        system_millis - offset
    }

    fn system_millis() -> i64 {
        let system_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime is before UNIX_EPOCH");
        system_time.as_millis() as i64
    }
}

impl<EGUnsignedReq, EGRes> std::fmt::Debug for Synchronization<EGUnsignedReq, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Synchronization")
            .field("create_request", &"<function>")
            .field("timeout", &self.timeout)
            .field("to_server_time", &"<function>")
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clock_applies_server_offset() {
        let clock = Clock::default();
        let local = clock.now_millis();
        clock.sync(local + 10_000, Duration::ZERO).unwrap();
        let synced = clock.now_millis();
        assert!(synced >= local + 10_000, "synced: {synced}");
        assert!(synced < local + 10_000 + 60_000, "synced: {synced}");
    }

    #[test]
    fn sync_returns_mutex_poisoned_instead_of_panicking() {
        let clock = Clock::default();
        let _ = std::panic::catch_unwind(|| {
            let mut guard = clock.last_sync.lock().unwrap();
            *guard = Some(Instant::now());
            panic!("poison the last_sync mutex");
        });
        let result = clock.sync(clock.now_millis(), Duration::ZERO);
        assert!(matches!(result, Err(EGError::MutexPoisoned)));
    }

    #[test]
    fn clone_recovers_from_poisoned_last_sync() {
        let clock = Clock::default();
        clock.sync(clock.now_millis(), Duration::ZERO).unwrap();
        let offset = clock.offset_millis();
        let _ = std::panic::catch_unwind(|| {
            let mut guard = clock.last_sync.lock().unwrap();
            *guard = Some(Instant::now());
            panic!("poison the last_sync mutex");
        });
        let cloned = clock.clone();
        assert_eq!(cloned.offset_millis(), offset);
    }
}
