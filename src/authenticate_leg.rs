use crate::{
    functions::{ArcCreateAuthMessage, ArcPredicate, TryConvertValue},
    sign::signer::Signer,
};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct AuthenticateLeg<EGUnsignedReq, EGReq, EGRes> {
    pub create_auth_message: ArcCreateAuthMessage<EGUnsignedReq>,
    pub timeout: Duration,
    pub filter: ArcPredicate<EGRes>,
    pub create_signer: TryConvertValue<EGRes, Signer<EGUnsignedReq, EGReq>>,
}

impl<EGUnsignedReq, EGReq, EGRes> std::fmt::Debug for AuthenticateLeg<EGUnsignedReq, EGReq, EGRes> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticateLeg")
            .field("create_auth_message", &"<function>")
            .field("timeout", &self.timeout)
            .field("filter_response", &"<function>")
            .field("create_signer", &self.create_signer)
            .finish()
    }
}
