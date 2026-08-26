use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    error::{EGError, EGResult},
    functions::{ArcTryConvertValue, TryConvertRef},
    rate_limit::rate_limits::RateLimits,
    sign::{
        convert_signer::ConvertSigner,
        signer::{Signer, SignerTrait},
    },
    transports::transport::{Transport, TransportTrait},
};
use async_trait::async_trait;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct ConnectorImpl<
    ExternalReq,
    EGUnsignedReq,
    TCredentials,
    EGReq,
    TransportReq,
    TransportRes,
    EGRes,
> {
    pub(crate) rate_limits: RateLimits,
    pub(crate) to_weight: fn(&EGUnsignedReq) -> u32,
    pub(crate) to_unsigned_request: ArcTryConvertValue<ExternalReq, EGUnsignedReq>,
    pub(crate) transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
    pub(crate) null_signer: ConvertSigner<EGUnsignedReq, EGReq>,
    pub(crate) credentials: Option<TCredentials>,
    pub(crate) create_signer: TryConvertRef<TCredentials, Signer<EGUnsignedReq, EGReq>>,
    pub(crate) authenticate_legs: Vec<AuthenticateLeg<EGUnsignedReq, EGReq, EGRes>>,
    pub(crate) signer: Arc<Mutex<Option<Signer<EGUnsignedReq, EGReq>>>>,
}

#[async_trait]
impl<
    ExternalReq,
    EGUnsignedReq,
    TCredentials,
    EGReq,
    TransportReq,
    TransportRes,
    EGRes,
    ExternalRes,
> Connector<ExternalReq, ExternalRes>
    for ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
    >
where
    ExternalReq: Send,
    TCredentials: Sync,
    EGReq: Send,
    TransportRes: Send,
    TransportReq: Send,
    EGRes: Send + Sync + 'static,
{
    async fn connect(&self) -> EGResult<()> {
        self.transport.connect().await?;
        if let Some(credentials) = &self.credentials {
            let mut signer = (self.create_signer)(credentials)?;
            for leg in &self.authenticate_legs {
                let auth_message = (leg.create_auth_message)();
                let signed_auth_message = signer.sign(auth_message)?;
                let authentication_response = self
                    .transport
                    .send_and_wait_for(signed_auth_message, leg.timeout, leg.filter.clone())
                    .await?;
                signer = (leg.create_signer)(authentication_response)?;
            }
            let mut guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            (*guard) = Some(signer);
        }
        Ok(())
    }
    fn is_connected(&self) -> EGResult<bool> {
        Ok(self.transport.is_connected())
    }
    fn is_authenticated(&self) -> EGResult<bool> {
        let guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
        Ok((*guard).is_some())
    }
    async fn disconnect(&self) -> EGResult<()> {
        {
            let mut guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            *guard = None;
        }
        self.transport.disconnect().await
    }
    async fn send(&self, request: ExternalReq, signed: bool, timeout: Duration) -> EGResult<()> {
        let (signed_request, weight) = {
            let unsigned = (self.to_unsigned_request)(request)?;
            self.check_rate_limits(&unsigned)?;
            let weight = (self.to_weight)(&unsigned);
            let signed_request = match self.signed_request(unsigned, signed) {
                Ok(signed_request) => signed_request,
                Err(error) => {
                    let _ = self.rate_limits.refund(weight);
                    return Err(error);
                }
            };
            (signed_request, weight)
        };
        match self
            .transport
            .fire_and_forget(signed_request, timeout)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self.rate_limits.refund(weight);
                Err(error)
            }
        }
    }
}
impl<ExternalReq, EGUnsignedReq, TCredentials, EGReq, TransportReq, TransportRes, EGRes>
    ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
    >
{
    fn signed_request(&self, unsigned: EGUnsignedReq, signed: bool) -> EGResult<EGReq> {
        if signed {
            let guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            match guard.deref() {
                None => Err(EGError::NotAuthenticated),
                Some(signer) => signer.sign(unsigned),
            }
        } else {
            self.null_signer.sign(unsigned)
        }
    }
    fn check_rate_limits(&self, unsigned: &EGUnsignedReq) -> EGResult<()> {
        let weight = (self.to_weight)(unsigned);
        if !self.rate_limits.request.did_acquire(weight)? {
            return Err(EGError::RateLimited);
        }
        Ok(())
    }
}

impl<ExternalReq, EGUnsignedReq, TCredentials, EGReq, TransportReq, TransportRes, EGRes>
    std::fmt::Debug
    for ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
    >
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("rate_limits", &self.rate_limits)
            .field("to_weight", &"<function>")
            .field("to_unsigned_request", &"<function>")
            .field("transport", &self.transport)
            .field("null_signer", &self.null_signer)
            .field("credentials", &"<redacted>")
            .field("create_signer", &self.create_signer)
            .field("authenticate_legs", &self.authenticate_legs)
            .field("signer", &"<redacted>")
            .finish()
    }
}
