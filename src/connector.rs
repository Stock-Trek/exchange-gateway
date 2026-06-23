use crate::{error::EGResult, exchange_spec::ExchangeSpec, sign::signer::Signer};
use async_trait::async_trait;
use std::marker::PhantomData;

pub trait SpecCreatorTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse> {
    fn into_spec_signer(
        self,
    ) -> EGResult<(
        ExchangeSpec<TRequest, TUnsignedMessage, TSignedMessage, TResponse>,
        Signer<TUnsignedMessage, TSignedMessage>,
    )>;
}

pub struct ConnectorCreator<S, TRequest, TUnsignedMessage, TSignedMessage, TResponse>
where
    S: SpecCreatorTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse> + Sized,
{
    pub(crate) spec_creator: S,
    pub(crate) _phantom_request: PhantomData<TRequest>,
    pub(crate) _phantom_unsigned_message: PhantomData<TUnsignedMessage>,
    pub(crate) _phantom_signed_message: PhantomData<TSignedMessage>,
    pub(crate) _phantom_response: PhantomData<TResponse>,
}

impl<S, TRequest, TUnsignedMessage, TSignedMessage, TResponse>
    ConnectorCreator<S, TRequest, TUnsignedMessage, TSignedMessage, TResponse>
where
    S: SpecCreatorTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse> + Sized,
    TRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub fn into_authenticator(self) -> EGResult<Authenticator<TRequest, TResponse>> {
        let (spec, signer) = self.spec_creator.into_spec_signer()?;
        Ok(Box::new(ConnectorImpl { spec, signer }))
    }
}

pub type Authenticator<TRequest, TResponse> = Box<dyn AuthenticatorTrait<TRequest, TResponse>>;
pub type Connector<TRequest, TResponse> = Box<dyn ConnectorTrait<TRequest, TResponse>>;

#[async_trait]
pub trait AuthenticatorTrait<TRequest, TResponse> {
    async fn authenticate(self) -> EGResult<Connector<TRequest, TResponse>>;
}
#[async_trait]
pub trait ConnectorTrait<TRequest, TResponse> {
    async fn send(&self, request: TRequest) -> EGResult<TResponse>;
}

pub struct ConnectorImpl<TRequest, TUnsignedMessage, TSignedMessage, TResponse> {
    spec: ExchangeSpec<TRequest, TUnsignedMessage, TSignedMessage, TResponse>,
    signer: Signer<TUnsignedMessage, TSignedMessage>,
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TSignedMessage, TResponse> AuthenticatorTrait<TRequest, TResponse>
    for ConnectorImpl<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
where
    TRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    async fn authenticate(self) -> EGResult<Connector<TRequest, TResponse>> {
        let ConnectorImpl { signer, spec } = self;
        let signer = spec.authenticate(signer).await?;
        Ok(Box::new(ConnectorImpl { signer, spec }))
    }
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TSignedMessage, TResponse> ConnectorTrait<TRequest, TResponse>
    for ConnectorImpl<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
where
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
{
    async fn send(&self, request: TRequest) -> EGResult<TResponse> {
        self.spec.send(request, &self.signer).await
    }
}
