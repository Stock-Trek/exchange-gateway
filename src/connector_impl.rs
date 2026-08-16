use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    error::{EGError, EGResult},
    functions::{TryConvertRef, TryConvertValue},
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
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
    pub(crate) request_weights: RequestWeights,
    pub(crate) to_unsigned_request: TryConvertValue<ExternalReq, EGUnsignedReq>,
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
            let mut signer = (self.create_signer)(&credentials)?;
            for leg in &self.authenticate_legs {
                let auth_message = (leg.create_auth_message)();
                let signed_auth_message = signer.sign(auth_message)?;
                let authentication_response = self
                    .transport
                    .send_and_wait_for(signed_auth_message, leg.timeout, leg.filter.clone())
                    .await?;
                signer = (leg.create_signer)(authentication_response)?;
            }
            let mut guard = self.signer.lock().map_err(|_| EGError::BadResponse)?;
            (*guard) = Some(signer);
        }
        Ok(())
    }
    fn is_connected(&self) -> EGResult<bool> {
        Ok(self.transport.is_connected())
    }
    fn is_authenticated(&self) -> EGResult<bool> {
        let guard = self.signer.lock().map_err(|_| EGError::BadResponse)?;
        Ok((*guard).is_some())
    }
    async fn disconnect(&self) -> EGResult<()> {
        {
            let mut guard = self.signer.lock().map_err(|_| EGError::BadResponse)?;
            *guard = None;
        }
        self.transport.disconnect().await
    }
    async fn send(&self, request: ExternalReq, signed: bool, timeout: Duration) -> EGResult<()> {
        self.check_rate_limits()?;
        let signed_request = self.signed_request(request, signed)?;
        self.transport
            .fire_and_forget(signed_request, timeout)
            .await
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
    fn signed_request(&self, request: ExternalReq, signed: bool) -> EGResult<EGReq> {
        let unsigned = (self.to_unsigned_request)(request)?;
        if signed {
            let guard = self.signer.lock().map_err(|_| EGError::BadResponse)?;
            match guard.deref() {
                None => Err(EGError::BadResponse),
                Some(signer) => signer.sign(unsigned),
            }
        } else {
            self.null_signer.sign(unsigned)
        }
    }
    fn check_rate_limits(&self) -> EGResult<()> {
        // TODO add rate limits back in
        // if !self
        //     .rate_limits
        //     .send_order_request
        //     .did_acquire(self.request_weights.send_order_request)
        // {
        //     return Err(EGError::BadResponse);
        // }
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
            .field("request_weights", &self.request_weights)
            .field("convert_request", &"<function>")
            .field("null_signer", &self.null_signer)
            .field("transport", &self.transport)
            .field("create_signer", &self.create_signer)
            .field("authenticate_legs", &self.authenticate_legs)
            .finish()
    }
}
