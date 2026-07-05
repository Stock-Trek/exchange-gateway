use crate::{
    connector_session::{ConnectorSession, ConnectorSessionImpl},
    error::EGResult,
    functions::{CreateAuthMessage, CreateSignerFrom, TryConvertRequestTo, TryConvertResponseFrom},
    listeners::{
        convert_listener::ConvertListener, listener::Listener, queue_listener::QueueListener,
    },
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::signer::Signer,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;
use std::sync::Arc;

pub type Connector<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TResponse,
> = Box<
    dyn ConnectorTrait<
            TRequest,
            TUnsignedMessageToExchange,
            TCredentials,
            TMessageToExchange,
            TResponse,
        >,
>;

#[async_trait]
pub trait ConnectorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TResponse,
>
{
    fn signer(
        &self,
        credentials: TCredentials,
    ) -> EGResult<Signer<TUnsignedMessageToExchange, TMessageToExchange>>;
    async fn request(
        &self,
        request: TRequest,
        signer: Option<Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
    ) -> EGResult<TResponse>;
    async fn into_session(
        self,
        credentials: TCredentials,
        listener: Listener<TResponse>,
    ) -> EGResult<ConnectorSession<TRequest>>;
}

pub(crate) struct ConnectorImpl<
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
    pub null_signer: Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    pub message_from_to_response: TryConvertResponseFrom<TMessageFromExchange, TResponse>,
    pub message_out_to_dto: TryConvertRequestTo<TMessageToExchange, TTransport::MessageDto>,
    pub transport: TTransport,
    pub transport_listener: Arc<ConvertListener<TTransport::MessageDto, TMessageFromExchange>>,
    pub queue_listener: Arc<QueueListener<TMessageFromExchange>>,
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
> ConnectorTrait<TRequest, TUnsignedMessageToExchange, TCredentials, TMessageToExchange, TResponse>
    for ConnectorImpl<
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
    fn signer(
        &self,
        credentials: TCredentials,
    ) -> EGResult<Signer<TUnsignedMessageToExchange, TMessageToExchange>> {
        (self.create_signer_from_credentials)(credentials)
    }
    async fn request(
        &self,
        request: TRequest,
        signer: Option<Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
    ) -> EGResult<TResponse> {
        let unsigned = (self.request_to_unsigned)(&request)?;
        let message_to = match signer {
            Some(signer) => signer.sign(unsigned),
            None => self.null_signer.sign(unsigned),
        }?;
        let request_dto = (self.message_out_to_dto)(&message_to)?;
        self.transport.send(request_dto, self.timeout).await?;
        let message_from = self.queue_listener.wait_for_message().await?;
        let response = (self.message_from_to_response)(message_from)?;
        Ok(response)
    }
    async fn into_session(
        self,
        credentials: TCredentials,
        listener: Listener<TResponse>,
    ) -> EGResult<ConnectorSession<TRequest>> {
        let mut signer = (self.create_signer_from_credentials)(credentials)?;
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let auth_message_dto = (self.message_out_to_dto)(&signed_auth_message)?;
            self.transport.send(auth_message_dto, leg.timeout).await?;
            let message_from = self.queue_listener.wait_for_message().await?;
            signer = (leg.create_signer_from)(message_from)?;
        }
        let ConnectorImpl {
            request_to_unsigned,
            null_signer,
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
        let connector_session = ConnectorSessionImpl {
            rate_limits,
            request_weights,
            request_to_unsigned,
            null_signer,
            signer,
            message_out_to_dto,
            transport,
            timeout,
        };
        Ok(Box::new(connector_session))
    }
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
