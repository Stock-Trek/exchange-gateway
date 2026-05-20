use crate::{
    auth_spec::AuthSpec,
    authenticate_leg::AuthenticateLegTrait,
    build::{
        authenticate_leg_builder::AuthenticateLegBuilder, message_leg_builder::MessageLegBuilder,
    },
    destroy::Destroy,
    message_leg::MessageLegTrait,
    transport::transport::Transport,
};
use chrono::Duration;

pub struct AuthSpecBuilder<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub authentication: Vec<Box<dyn AuthenticateLegTrait<TState, TCredentials, TTransports>>>,
    pub message: Box<dyn MessageLegTrait<TState, TCredentials, TTransports, TMessage, TReply>>,
}

impl<TState, TCredentials, TTransports, TMessage, TReply>
    AuthSpecBuilder<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        message: Box<dyn MessageLegTrait<TState, TCredentials, TTransports, TMessage, TReply>>,
    ) -> Self {
        Self {
            authentication: Vec::new(),
            message,
        }
    }
    pub fn begin_authentication_leg<TAuthTransport, TAuthMessage, TAuthReply>(
        mut self,
        get_transport: fn(transports: &TTransports) -> &TAuthTransport,
        timeout: Duration,
    ) -> AuthenticateLegBuilder<
        TState,
        TCredentials,
        TTransports,
        TMessage,
        TReply,
        TAuthTransport,
        TAuthMessage,
        TAuthReply,
    >
    where
        TAuthTransport: Transport<TAuthMessage, TAuthReply> + Send + Sync + 'static,
        TAuthMessage: Send + Sync + 'static,
        TAuthReply: Send + Sync + 'static,
    {
        let authentication = std::mem::take(&mut self.authentication);
        let builder = AuthSpecBuilder {
            authentication,
            message: self.message,
        };
        AuthenticateLegBuilder::new(builder, get_transport, timeout)
    }
    pub fn begin_message_leg<TTransport>(
        mut self,
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
    ) -> MessageLegBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    where
        TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
        TMessage: Send + Sync + 'static,
        TReply: Send + Sync + 'static,
    {
        let authentication = std::mem::take(&mut self.authentication);
        let builder = AuthSpecBuilder {
            authentication,
            message: self.message,
        };
        MessageLegBuilder::new(builder, get_transport, timeout)
    }
    pub fn build_spec(&mut self) -> AuthSpec<TState, TCredentials, TTransports, TMessage, TReply> {
        AuthSpec::<TState, TCredentials, TTransports, TMessage, TReply>::new(
            std::mem::take(&mut self.authentication),
            self.message,
        )
    }
}
