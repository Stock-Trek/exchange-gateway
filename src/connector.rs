use crate::{
    connector_session::ConnectorSession,
    error::EGResult,
    functions::{CreateAuthMessage, CreateSignerFrom, FilterMessage, TryConvertRequestTo},
    listeners::{
        exchange_listener::ExchangeListener, one_shot_interceptor::OneShotInterceptorImpl,
    },
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::signer::Signer,
    transports::transport::TransportTrait,
};
use std::{sync::Arc, time::Duration};

pub struct Connector<
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
    pub(crate) rate_limits: RateLimits,
    pub(crate) request_weights: RequestWeights,
    pub(crate) request_to_unsigned: TryConvertRequestTo<TRequest, TUnsignedMessageToExchange>,
    pub(crate) null_signer: Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) message_out_to_dto: TryConvertRequestTo<TMessageToExchange, TTransport::MessageDto>,
    pub(crate) transport: TTransport,
    pub(crate) listener: Arc<ExchangeListener<TTransport::MessageDto, TResponse>>,
    pub(crate) create_signer_from_credentials:
        CreateSignerFrom<TCredentials, TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) authenticate_legs:
        Vec<AuthenticateLeg<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange>>,
    pub(crate) timeout: Duration,
}

impl<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TTransport,
    TMessageFromExchange,
    TResponse,
>
    Connector<
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
    pub fn signer(
        &self,
        credentials: TCredentials,
    ) -> EGResult<Signer<TUnsignedMessageToExchange, TMessageToExchange>> {
        (self.create_signer_from_credentials)(credentials)
    }
    pub async fn request(
        &self,
        request: TRequest,
        signer: Option<Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
    ) -> EGResult<()> {
        let unsigned = (self.request_to_unsigned)(&request)?;
        let message_to = match signer {
            Some(signer) => signer.sign(unsigned),
            None => self.null_signer.sign(unsigned),
        }?;
        let request_dto = (self.message_out_to_dto)(&message_to)?;
        self.transport.send(request_dto, self.timeout).await
    }
    pub async fn request_and_wait<TWaitedResponse>(
        &self,
        request: TRequest,
        signer: Option<Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
        filter_response: FilterMessage<TResponse, TWaitedResponse>,
        timeout: Duration,
    ) -> EGResult<TWaitedResponse>
    where
        TWaitedResponse: Send + 'static,
    {
        let unsigned = (self.request_to_unsigned)(&request)?;
        let message_to = match signer {
            Some(signer) => signer.sign(unsigned),
            None => self.null_signer.sign(unsigned),
        }?;
        let request_dto = (self.message_out_to_dto)(&message_to)?;
        let interceptor = Arc::new(OneShotInterceptorImpl::new(filter_response));
        self.listener.add_interceptor(interceptor.clone());
        self.transport.send(request_dto, self.timeout).await?;
        interceptor.wait(timeout)
    }
    pub async fn into_session(
        self,
        credentials: TCredentials,
    ) -> EGResult<
        ConnectorSession<TRequest, TUnsignedMessageToExchange, TMessageToExchange, TTransport>,
    > {
        let mut signer = (self.create_signer_from_credentials)(credentials)?;
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let auth_message_dto = (self.message_out_to_dto)(&signed_auth_message)?;

            let interceptor = Arc::new(OneShotInterceptorImpl::new(filter_response));
            self.listener.add_interceptor(interceptor.clone());

            self.transport.send(auth_message_dto, leg.timeout).await?;

            let authentication_response = interceptor.wait(timeout)?;

            signer = (leg.create_signer_from)(authentication_response)?;
        }
        let Connector {
            rate_limits,
            request_weights,
            request_to_unsigned,
            null_signer,
            message_out_to_dto,
            transport,
            timeout,
            ..
        } = self;
        let connector_session = ConnectorSession {
            rate_limits,
            request_weights,
            request_to_unsigned,
            null_signer,
            signer,
            message_out_to_dto,
            transport,
            timeout,
        };
        Ok(connector_session)
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
