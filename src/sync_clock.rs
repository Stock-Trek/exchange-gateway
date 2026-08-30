use crate::functions::{ArcCreateAuthAttempt, ArcTryConvertValue};
use std::time::Duration;

/// The capability to sync the server clock from the exchange's unsigned
/// `time` endpoint. `create_request` builds the request (with a filter that
/// matches its response), `sync` re-adopts the server's offset once the
/// response and its round-trip time are known.
pub(crate) struct SyncClock<EGUnsignedReq, EGRes> {
    pub create_request: ArcCreateAuthAttempt<EGUnsignedReq, EGRes>,
    pub timeout: Duration,
    pub sync: ArcTryConvertValue<(EGRes, Duration), ()>,
}

impl<EGUnsignedReq, EGRes> std::fmt::Debug for SyncClock<EGUnsignedReq, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncClock")
            .field("create_request", &"<function>")
            .field("timeout", &self.timeout)
            .field("sync", &"<function>")
            .finish()
    }
}
