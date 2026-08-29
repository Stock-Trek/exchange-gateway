use crate::{
    functions::{ArcCreateAuthAttempt, ArcTryConvertValue},
    sign::signer::Signer,
};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct AuthenticateLeg<EGUnsignedReq, EGReq, EGRes> {
    pub create_auth_attempt: ArcCreateAuthAttempt<EGUnsignedReq, EGRes>,
    pub timeout: Duration,
    /// Builds the signer the remaining authentication legs (and ultimately
    /// user requests) run through. A leg that only gathers information —
    /// e.g. a server-time bootstrap before the logon — returns `Ok(None)`
    /// to keep the signer the previous leg installed.
    pub create_signer: ArcTryConvertValue<(EGRes, Duration), Option<Signer<EGUnsignedReq, EGReq>>>,
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
