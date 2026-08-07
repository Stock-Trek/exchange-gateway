use crate::{
    connector_session::ConnectorSession,
    error::EGResult,
    functions::{ArcTryConvertValue, TryConvertRef, TryConvertValue},
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::{
        convert_signer::ConvertSigner,
        signer::{Signer, SignerTrait},
    },
    transports::transport::Transport,
};
use std::time::Duration;

pub struct Connector<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
> where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    pub(crate) rate_limits: RateLimits,
    pub(crate) request_weights: RequestWeights,
    pub(crate) request_to_unsigned: ArcTryConvertValue<TRequest, TUnsignedMessageToExchange>,
    pub(crate) null_signer: ConvertSigner<TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) transport: Transport<TMessageToExchange, TMessageFromExchange, TResponse>,
    pub(crate) create_signer_from_credentials:
        TryConvertRef<TCredentials, Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
    pub(crate) authenticate_legs:
        Vec<AuthenticateLeg<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange>>,
}

impl<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
>
    Connector<
        TRequest,
        TUnsignedMessageToExchange,
        TCredentials,
        TMessageToExchange,
        TMessageFromExchange,
        TResponse,
    >
where
    TRequest: Send + Sync + 'static,
    TUnsignedMessageToExchange: Send + Sync + 'static,
    TCredentials: Send + Sync + 'static,
    TMessageToExchange: Send + Sync + 'static,
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub fn signer(
        &self,
        credentials: &TCredentials,
    ) -> EGResult<Signer<TUnsignedMessageToExchange, TMessageToExchange>> {
        (self.create_signer_from_credentials)(credentials)
    }
    pub async fn connect(&self) -> EGResult<()> {
        self.transport.connect().await
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        self.transport.disconnect().await
    }
    pub async fn fire_and_forget(
        &self,
        request: TRequest,
        signer: Option<&Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
        timeout: Duration,
    ) -> EGResult<()> {
        let unsigned = (self.request_to_unsigned)(request)?;
        let message_to = match signer {
            Some(signer) => signer.sign(unsigned),
            None => self.null_signer.sign(unsigned),
        }?;
        self.transport.fire_and_forget(message_to, timeout).await
    }
    pub async fn send_and_wait<TWaitedResponse>(
        &self,
        request: TRequest,
        signer: Option<&Signer<TUnsignedMessageToExchange, TMessageToExchange>>,
        filter_response: ArcTryConvertValue<TResponse, TWaitedResponse>,
        timeout: Duration,
    ) -> EGResult<TWaitedResponse>
    where
        TWaitedResponse: Send + Sync + 'static,
    {
        let unsigned = (self.request_to_unsigned)(request)?;
        let message_to = match signer {
            Some(signer) => signer.sign(unsigned),
            None => self.null_signer.sign(unsigned),
        }?;
        self.transport
            .send_and_wait_for_response(message_to, timeout, filter_response)
            .await
    }
    pub async fn into_session(
        self,
        credentials: &TCredentials,
    ) -> EGResult<
        ConnectorSession<
            TRequest,
            TUnsignedMessageToExchange,
            TMessageToExchange,
            TMessageFromExchange,
            TResponse,
        >,
    > {
        let mut signer = (self.create_signer_from_credentials)(credentials)?;
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let authentication_response = self
                .transport
                .send_and_wait_for_message_from(
                    signed_auth_message,
                    leg.timeout,
                    leg.filter_response.clone(),
                )
                .await?;
            signer = (leg.create_signer_from)(authentication_response)?;
        }
        let Connector {
            rate_limits,
            request_weights,
            request_to_unsigned,
            null_signer,
            transport,
            ..
        } = self;
        let connector_session = ConnectorSession {
            rate_limits,
            request_weights,
            request_to_unsigned,
            null_signer,
            signer,
            transport,
        };
        Ok(connector_session)
    }
}

impl<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
> std::fmt::Debug
    for Connector<
        TRequest,
        TUnsignedMessageToExchange,
        TCredentials,
        TMessageToExchange,
        TMessageFromExchange,
        TResponse,
    >
where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("rate_limits", &self.rate_limits)
            .field("request_weights", &self.request_weights)
            .field("request_to_unsigned", &"<function>")
            .field("null_signer", &self.null_signer)
            .field("transport", &self.transport)
            .field(
                "create_signer_from_credentials",
                &self.create_signer_from_credentials,
            )
            .field("authenticate_legs", &self.authenticate_legs)
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct AuthenticateLeg<
    TUnsignedMessageToExchange,
    TMessageToExchange,
    TMessageFromExchange,
> {
    pub create_auth_message: fn() -> TUnsignedMessageToExchange,
    pub timeout: Duration,
    pub filter_response: ArcTryConvertValue<TMessageFromExchange, TMessageFromExchange>,
    pub create_signer_from: TryConvertValue<
        TMessageFromExchange,
        Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    >,
}

impl<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange> std::fmt::Debug
    for AuthenticateLeg<TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticateLeg")
            .field("create_auth_message", &self.create_auth_message)
            .field("timeout", &self.timeout)
            .field("filter_response", &"<function>")
            .field("create_signer_from", &self.create_signer_from)
            .finish()
    }
}
