use crate::{
    clock::Clock,
    functions::{ArcConvertRef, ArcPredicate, ArcTryConvertValue},
    sign::signer::Signer,
};
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct AuthenticateLeg<EGUnsignedReq, EGReq, EGRes> {
    pub create_auth_attempt: ArcConvertRef<Clock, (EGUnsignedReq, ArcPredicate<EGRes>)>,
    pub timeout: Duration,
    pub create_signer: ArcTryConvertValue<EGRes, Option<Signer<EGUnsignedReq, EGReq>>>,
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
