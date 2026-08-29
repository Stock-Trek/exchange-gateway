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
    last_sync: Mutex<Instant>,
    sync_interval: Duration,
}

impl Default for Clock {
    fn default() -> Self {
        Clock::new(Duration::from_mins(1))
    }
}

impl Clock {
    pub fn new(sync_interval: Duration) -> Self {
        Self {
            offset_millis: AtomicI64::new(0),
            last_sync: Mutex::new(Instant::now() - sync_interval),
            sync_interval,
        }
    }

    pub fn offset_millis(&self) -> i64 {
        self.offset_millis.load(Ordering::Relaxed)
    }

    pub fn should_sync(&self) -> bool {
        let last = *self.last_sync.lock().unwrap();
        last.elapsed() >= self.sync_interval
    }

    pub fn sync(&self, server_time_millis: i64, round_trip_time: Duration) {
        let rtt_ms = round_trip_time.as_millis() as i64;
        let system_millis = Self::system_millis();
        let midpoint_system_millis = system_millis - (rtt_ms / 2);
        let new_offset = midpoint_system_millis - server_time_millis;
        self.offset_millis.store(new_offset, Ordering::Relaxed);
        *self.last_sync.lock().unwrap() = Instant::now();
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clock_applies_server_offset() {
        let clock = Clock::default();
        let local = clock.now_millis();
        clock.sync(local + 10_000, Duration::ZERO);
        let synced = clock.now_millis();
        assert!(synced >= local + 10_000, "synced: {synced}");
        assert!(synced < local + 10_000 + 60_000, "synced: {synced}");
    }
}
