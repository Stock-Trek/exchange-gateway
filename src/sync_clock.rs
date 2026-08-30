use crate::functions::{ArcCreateAuthAttempt, ArcTryConvertValueWithClock};
use std::time::Duration;

pub(crate) struct SyncClock<EGUnsignedReq, EGRes> {
    pub create_request: ArcCreateAuthAttempt<EGUnsignedReq, EGRes>,
    pub timeout: Duration,
    pub sync: ArcTryConvertValueWithClock<(EGRes, Duration), ()>,
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
