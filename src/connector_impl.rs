use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue, TryConvertRef, TryConvertValue},
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
    ExternalRes,
> {
    pub(crate) rate_limits: RateLimits,
    pub(crate) request_weights: RequestWeights,
    pub(crate) to_unsigned_request: TryConvertValue<ExternalReq, EGUnsignedReq>,
    pub(crate) to_external_response: ArcTryConvertValue<EGRes, ExternalRes>,
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
        ExternalRes,
    >
where
    ExternalReq: Send,
    TCredentials: Sync,
    EGReq: Send,
    TransportRes: Send,
    TransportReq: Send,
    EGRes: Send + Sync + Clone + 'static,
    ExternalRes: Send + Sync + 'static,
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
    async fn send(
        &self,
        request: ExternalReq,
        signed: bool,
        timeout: Duration,
        filter: ArcPredicate<ExternalRes>,
    ) -> EGResult<ExternalRes> {
        self.check_rate_limits()?;
        let signed_request = self.signed_request(request, signed)?;
        let to_external_response = self.to_external_response.clone();
        let response_filter: ArcPredicate<EGRes> = Arc::new(move |response| {
            to_external_response(response.clone())
                .map(|external_response| filter(&external_response))
                .unwrap_or(false)
        });
        let internal_response = self
            .transport
            .send_and_wait_for(signed_request, timeout, response_filter)
            .await?;
        (self.to_external_response)(internal_response)
    }
}
impl<ExternalReq, EGUnsignedReq, TCredentials, EGReq, TransportReq, TransportRes, EGRes, ExternalRes>
    ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
        ExternalRes,
    >
{
    fn signed_request(&self, request: ExternalReq, signed: bool) -> EGResult<EGReq> {
        let unsigned = (self.to_unsigned_request)(request)?;
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
    fn check_rate_limits(&self) -> EGResult<()> {
        if !self
            .rate_limits
            .send_order_request
            .did_acquire(self.request_weights.send_order_request)
        {
            return Err(EGError::RateLimited);
        }
        Ok(())
    }
}

impl<ExternalReq, EGUnsignedReq, TCredentials, EGReq, TransportReq, TransportRes, EGRes, ExternalRes>
    std::fmt::Debug
    for ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
        ExternalRes,
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
