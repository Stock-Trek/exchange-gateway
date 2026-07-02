use crate::{
    connector::{Connector, ConnectorImpl},
    converter::Converter,
    error::EGResult,
    functions::{CreateAuthMessage, CreateSignerFrom},
    listeners::hybrid_listener::{HybridListener, ListenMode},
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;

pub type Authenticator<TRequest, TCredentials, TResponse> =
    Box<dyn AuthenticatorTrait<TRequest, TCredentials, TResponse>>;

#[async_trait]
pub trait AuthenticatorTrait<TRequest, TCredentials, TResponse> {
    async fn increments(&self) -> EGResult<TResponse>;
    async fn authenticate(
        self,
        credentials: TCredentials,
    ) -> EGResult<Connector<TRequest, TResponse>>;
}

pub struct AuthenticatorImpl<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TTransport,
    TMessageFromExchange,
    TResponse,
> where
    TTransport: TransportTrait,
{
    pub(crate) exchange_converter:
        Converter<TRequest, TUnsignedMessageToExchange, TMessageFromExchange, TResponse>,
    pub(crate) dto_converter: Converter<
        TMessageToExchange,
        TTransport::MessageDto,
        TTransport::MessageDto,
        TMessageFromExchange,
    >,
    pub(crate) transport: TTransport,
    pub(crate) wait_listener: HybridListener<TTransport::MessageDto>,
    pub(crate) increments_leg: IncrementsLeg<TMessageToExchange>,
    pub(crate) create_signer_from_credentials:
        CreateSignerFrom<TCredentials, TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) authenticate_legs:
        Vec<AuthenticateLeg<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange>>,
    pub(crate) connector_timeout: Duration,
    pub(crate) request_weights: RequestWeights,
    pub(crate) rate_limits: RateLimits,
}

#[async_trait]
impl<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TTransport,
    TMessageFromExchange,
    TResponse,
> AuthenticatorTrait<TRequest, TCredentials, TResponse>
    for AuthenticatorImpl<
        TRequest,
        TUnsignedMessageToExchange,
        TCredentials,
        TMessageToExchange,
        TTransport,
        TMessageFromExchange,
        TResponse,
    >
where
    TRequest: Send + Sync + 'static,
    TUnsignedMessageToExchange: Send + Sync + 'static,
    TCredentials: Send + Sync + 'static,
    TMessageToExchange: Send + Sync + 'static,
    TTransport: TransportTrait + Send + Sync + 'static,
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    async fn increments(&self) -> EGResult<TResponse> {
        self.wait_listener.mode(ListenMode::OnDemand)?;
        let request_dto = self
            .dto_converter
            .convert_req(&self.increments_leg.message)?;
        self.transport
            .send(request_dto, self.increments_leg.timeout)
            .await?;
        let message_dto = self.wait_listener.wait_for_message().await?;
        let message_from = self.dto_converter.convert_res(message_dto)?;
        let response = self.exchange_converter.convert_res(message_from)?;
        Ok(response)
    }
    async fn authenticate(
        self,
        credentials: TCredentials,
    ) -> EGResult<Connector<TRequest, TResponse>> {
        self.wait_listener.mode(ListenMode::OnDemand)?;
        let mut signer = (self.create_signer_from_credentials)(credentials)?;
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let auth_message_dto = self.dto_converter.convert_req(&signed_auth_message)?;
            self.transport.send(auth_message_dto, leg.timeout).await?;
            let message_dto = self.wait_listener.wait_for_message().await?;
            let message_from = self.dto_converter.convert_res(message_dto)?;
            signer = (leg.create_signer_from)(message_from)?;
        }
        self.wait_listener.mode(ListenMode::EventDriven)?;
        let connector = ConnectorImpl {
            rate_limits: self.rate_limits,
            request_weights: self.request_weights,
            exchange_converter: self.exchange_converter,
            signer,
            dto_converter: self.dto_converter,
            transport: self.transport,
            timeout: self.connector_timeout,
        };
        Ok(Box::new(connector))
    }
}

pub struct IncrementsLeg<TMessageToExchange> {
    pub(crate) message: TMessageToExchange,
    pub(crate) timeout: Duration,
}

pub struct AuthenticateLeg<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange> {
    pub(crate) create_auth_message: CreateAuthMessage<TUnsignedMessageToExchange>,
    pub(crate) timeout: Duration,
    pub(crate) create_signer_from:
        CreateSignerFrom<TMessageFromExchange, TUnsignedMessageToExchange, TMessageToExchange>,
}
