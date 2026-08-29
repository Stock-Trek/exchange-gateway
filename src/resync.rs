use crate::functions::{ArcCreateAuthAttempt, ArcTryConvertValue};
use std::time::Duration;

/// The capability to re-sync the server clock from the exchange's unsigned
/// `time` endpoint. `create_request` builds the request (with a filter that
/// matches its response), `sync_clock` re-adopts the server's offset once the
/// response and its round-trip time are known.
pub(crate) struct Resync<EGUnsignedReq, EGRes> {
    pub create_request: ArcCreateAuthAttempt<EGUnsignedReq, EGRes>,
    pub timeout: Duration,
    pub sync_clock: ArcTryConvertValue<(EGRes, Duration), ()>,
}

impl<EGUnsignedReq, EGRes> std::fmt::Debug for Resync<EGUnsignedReq, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resync")
            .field("create_request", &"<function>")
            .field("timeout", &self.timeout)
            .field("sync_clock", &"<function>")
            .finish()
    }
}
