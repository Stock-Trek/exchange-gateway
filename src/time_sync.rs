use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Default)]
pub(crate) struct TimeSync {
    offset_millis: AtomicI64,
}

impl TimeSync {
    pub fn sync(&self, server_time_millis: i64) {
        self.offset_millis.store(
            server_time_millis - Self::system_time_millis(),
            Ordering::SeqCst,
        );
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
