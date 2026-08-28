use crate::{
    functions::{ArcCreateAuthAttempt, ArcTryConvertValue},
    sign::signer::Signer,
};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct AuthenticateLeg<EGUnsignedReq, EGReq, EGRes> {
    /// Creates the next authentication message together with the filter that
    /// matches its response. Both are produced per attempt so a retry can use
    /// a fresh request id: a logon queued by a timed-out attempt must never
    /// resolve a newer attempt's waiter, and the newer attempt's own response
    /// must never leak to the user's listener.
    pub create_auth_attempt: ArcCreateAuthAttempt<EGUnsignedReq, EGRes>,
    pub timeout: Duration,
    pub create_signer: ArcTryConvertValue<EGRes, Signer<EGUnsignedReq, EGReq>>,
}

impl<EGUnsignedReq, EGReq, EGRes> std::fmt::Debug for AuthenticateLeg<EGUnsignedReq, EGReq, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticateLeg")
            .field("create_auth_attempt", &"<function>")
            .field("timeout", &self.timeout)
            .field("create_signer", &"<function>")
            .finish()
    }
}
