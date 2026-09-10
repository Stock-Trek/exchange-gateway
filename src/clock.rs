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

    pub fn duration_since_last_sync(&self) -> EGResult<Duration> {
        let result = self.last_sync.lock();
        let mutex = result.map_err(|_| EGError::MutexPoisoned)?;
        let last_sync = *mutex;
        Ok(last_sync.map_or(Duration::MAX, |i| i.elapsed()))
    }

    pub fn sync(&self, server_time: Milliseconds, round_trip_duration: Duration) -> EGResult<()> {
        let system_time = Self::system_time();
        let round_trip_time = Milliseconds(round_trip_duration.as_millis() as i64);
        let system_time_estimate = system_time - (round_trip_time / 2);
        let offset_estimate = system_time_estimate - server_time;
        self.server_offset_millis
            .store(offset_estimate.0, Ordering::Relaxed);
        let mut last_sync = self.last_sync.lock().map_err(|_| EGError::MutexPoisoned)?;
        *last_sync = Some(Instant::now());
        Ok(())
    }
    pub fn server_time_estimate(&self) -> Milliseconds {
        Self::system_time() - self.server_offset()
    }

    fn system_time() -> Milliseconds {
        let system_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime is before UNIX_EPOCH");
        Milliseconds(system_time.as_millis() as i64)
    }
    fn server_offset(&self) -> Milliseconds {
        Milliseconds(self.server_offset_millis.load(Ordering::Relaxed))
    }
}
