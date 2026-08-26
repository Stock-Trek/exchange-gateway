use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// Tracks the offset between the exchange's server clock and the local clock
/// so that signed requests can be stamped with a server-accurate timestamp
/// even if the local clock drifts.
#[derive(Debug, Default)]
pub(crate) struct TimeSync {
    offset_ms: AtomicI64,
}

impl TimeSync {
    /// Records the offset between the server clock and the local clock from a
    /// server-observed timestamp (milliseconds since the Unix epoch).
    pub fn sync(&self, server_time_ms: i64) {
        self.offset_ms
            .store(server_time_ms - now_ms(), Ordering::SeqCst);
    }

    /// The current server time in milliseconds since the Unix epoch, i.e. the
    /// local time adjusted by the last synced offset.
    pub fn now_ms(&self) -> i64 {
        now_ms() + self.offset_ms.load(Ordering::SeqCst)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before the Unix epoch")
        .as_millis()
        .try_into()
        .expect("System time does not fit in i64 milliseconds")
}
