use crate::{
    connector::{Connector, ConnectorImpl, RateLimits, RequestWeights},
    error::EGResult,
    functions::{CreateAuthMessage, CreateSignerFrom, TryConvertFromRequest, TryConvertToResponse},
    messenger::Messenger,
};
use async_trait::async_trait;
use chrono::Duration;
use std::marker::PhantomData;

pub type Authenticator<TRequest, TUnsignedMessage, TCredentials, TResponse> =
    Box<dyn AuthenticatorTrait<TRequest, TUnsignedMessage, TCredentials, TResponse>>;

#[async_trait]
pub trait AuthenticatorTrait<TRequest, TUnsignedMessage, TCredentials, TResponse> {
    async fn increments(&self) -> EGResult<TResponse>;
    async fn authenticate(
        self,
        credentials: TCredentials,
        request_converter: TryConvertFromRequest<TRequest, TUnsignedMessage>,
    ) -> EGResult<Connector<TRequest, TResponse>>;
}

pub struct AuthenticatorImpl<
    TRequest,
    TUnsignedMessage,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
> {
    pub(crate) messenger: Messenger<TMessageToExchange, TMessageFromExchange>,
    pub(crate) increments_leg: IncrementsLeg<TMessageToExchange>,
    pub(crate) create_signer_from_credentials:
        CreateSignerFrom<TCredentials, TUnsignedMessage, TMessageToExchange>,
    pub(crate) authenticate_legs:
        Vec<AuthenticateLeg<TUnsignedMessage, TMessageToExchange, TMessageFromExchange>>,
    pub(crate) connector_timeout: Duration,
    pub(crate) request_weights: RequestWeights,
    pub(crate) rate_limits: RateLimits,
    pub(crate) to_response: TryConvertToResponse<TMessageFromExchange, TResponse>,
    pub(crate) _phantom_request: PhantomData<TRequest>,
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TCredentials, TMessageToExchange, TMessageFromExchange, TResponse>
    AuthenticatorTrait<TRequest, TUnsignedMessage, TCredentials, TResponse>
    for AuthenticatorImpl<
        TRequest,
        TUnsignedMessage,
        TCredentials,
        TMessageToExchange,
        TMessageFromExchange,
        TResponse,
    >
where
    TRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TCredentials: Send + Sync + 'static,
    TMessageToExchange: Send + Sync + 'static,
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    async fn increments(&self) -> EGResult<TResponse> {
        let message_from = self
            .messenger
            .send(&self.increments_leg.message, self.increments_leg.timeout)
            .await?;
        let response = ((self.to_response)(message_from))?;
        Ok(response)
    }
    async fn authenticate(
        self,
        credentials: TCredentials,
        request_converter: TryConvertFromRequest<TRequest, TUnsignedMessage>,
    ) -> EGResult<Connector<TRequest, TResponse>> {
        let mut signer = (self.create_signer_from_credentials)(credentials)?;
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let message_from = self
                .messenger
                .send(&signed_auth_message, leg.timeout)
                .await?;
            signer = (leg.create_signer_from)(message_from)?;
        }
        let connector = ConnectorImpl {
            messenger: self.messenger,
            to_unsigned_message: request_converter,
            signer,
            timeout: self.connector_timeout,
            to_response: self.to_response,
            request_weights: self.request_weights.clone(),
            rate_limits: self.rate_limits.clone(),
        };
        Ok(Box::new(connector))
    }
}

pub struct IncrementsLeg<TMessageToExchange> {
    pub(crate) message: TMessageToExchange,
    pub(crate) timeout: Duration,
}

pub struct AuthenticateLeg<TUnsignedMessage, TMessageToExchange, TMessageFromExchange> {
    pub(crate) create_auth_message: CreateAuthMessage<TUnsignedMessage>,
    pub(crate) timeout: Duration,
    pub(crate) create_signer_from:
        CreateSignerFrom<TMessageFromExchange, TUnsignedMessage, TMessageToExchange>,
}
