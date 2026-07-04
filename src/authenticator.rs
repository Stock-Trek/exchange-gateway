use crate::{
    connector::{Connector, ConnectorImpl},
    error::EGResult,
    functions::{CreateAuthMessage, CreateSignerFrom, TryConvertRequestTo, TryConvertResponseFrom},
    listeners::{
        convert_listener::ConvertListener, listener::Listener, queue_listener::QueueListener,
    },
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;
use std::sync::Arc;

pub type Authenticator<TRequest, TCredentials, TResponse> =
    Box<dyn AuthenticatorTrait<TRequest, TCredentials, TResponse>>;

#[async_trait]
pub trait AuthenticatorTrait<TRequest, TCredentials, TResponse> {
    async fn increments(&self) -> EGResult<TResponse>;
    async fn authenticate(
        self,
        credentials: TCredentials,
        listener: Listener<TResponse>,
    ) -> EGResult<Connector<TRequest>>;
}

pub(crate) struct AuthenticatorImpl<
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
    pub request_to_unsigned: TryConvertRequestTo<TRequest, TUnsignedMessageToExchange>,
    pub message_from_to_response: TryConvertResponseFrom<TMessageFromExchange, TResponse>,
    pub message_out_to_dto: TryConvertRequestTo<TMessageToExchange, TTransport::MessageDto>,
    pub transport: TTransport,
    pub transport_listener: Arc<ConvertListener<TTransport::MessageDto, TMessageFromExchange>>,
    pub queue_listener: Arc<QueueListener<TMessageFromExchange>>,
    pub increments_leg: IncrementsLeg<TMessageToExchange>,
    pub create_signer_from_credentials:
        CreateSignerFrom<TCredentials, TUnsignedMessageToExchange, TMessageToExchange>,
    pub authenticate_legs:
        Vec<AuthenticateLeg<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange>>,
    pub timeout: Duration,
    pub request_weights: RequestWeights,
    pub rate_limits: RateLimits,
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
        let request_dto = (self.message_out_to_dto)(&self.increments_leg.message)?;
        self.transport
            .send(request_dto, self.increments_leg.timeout)
            .await?;
        let message_from = self.queue_listener.wait_for_message().await?;
        let response = (self.message_from_to_response)(message_from)?;
        Ok(response)
    }
    async fn authenticate(
        self,
        credentials: TCredentials,
        listener: Listener<TResponse>,
    ) -> EGResult<Connector<TRequest>> {
        let mut signer = (self.create_signer_from_credentials)(credentials)?;
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let auth_message_dto = (self.message_out_to_dto)(&signed_auth_message)?;
            self.transport.send(auth_message_dto, leg.timeout).await?;
            let message_from = self.queue_listener.wait_for_message().await?;
            signer = (leg.create_signer_from)(message_from)?;
        }
        let AuthenticatorImpl {
            request_to_unsigned,
            message_from_to_response,
            message_out_to_dto,
            transport,
            transport_listener,
            timeout,
            request_weights,
            rate_limits,
            ..
        } = self;
        let delegate_listener = ConvertListener::new(message_from_to_response, listener);
        transport_listener.set_delegate(Arc::new(delegate_listener))?;
        let connector = ConnectorImpl {
            rate_limits,
            request_weights,
            request_to_unsigned,
            signer,
            message_out_to_dto,
            transport,
            timeout,
        };
        Ok(Box::new(connector))
    }
}

pub(crate) struct IncrementsLeg<TMessageToExchange> {
    pub message: TMessageToExchange,
    pub timeout: Duration,
}

pub(crate) struct AuthenticateLeg<
    TUnsignedMessageToExchange,
    TMessageToExchange,
    TMessageFromExchange,
> {
    pub create_auth_message: CreateAuthMessage<TUnsignedMessageToExchange>,
    pub timeout: Duration,
    pub create_signer_from:
        CreateSignerFrom<TMessageFromExchange, TUnsignedMessageToExchange, TMessageToExchange>,
}
