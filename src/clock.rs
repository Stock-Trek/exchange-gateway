use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Default)]
pub(crate) struct Clock {
    offset_millis: AtomicI64,
}

impl Clock {
    pub fn sync(&self, server_time_millis: i64) {
        self.offset_millis.store(
            server_time_millis - Self::system_time_millis(),
            Ordering::SeqCst,
        );
    }

    pub fn now(&self) -> Instant {
        Instant::now()
    }

    pub fn now_millis(&self) -> i64 {
        Self::system_time_millis() + self.offset_millis.load(Ordering::SeqCst)
    }

    fn system_time_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before the Unix epoch")
            .as_millis()
            .try_into()
            .expect("System time does not fit in i64 milliseconds")
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn clock_applies_server_offset() {
        let clock = Clock::default();
        let local = clock.now_millis();
        clock.sync(local + 10_000);
        let synced = clock.now_millis();
        assert!(synced >= local + 10_000, "synced: {synced}");
        assert!(synced < local + 10_000 + 60_000, "synced: {synced}");
    }
}
