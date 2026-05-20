use crate::{
    auth_spec::AuthSpec, authenticate_leg::AuthenticateLegTrait,
    build::authenticate_leg_builder::AuthenticateLegBuilder, destroy::Destroy,
    message_leg::MessageLegGeneric, transport::transport::Transport,
};
use chrono::Duration;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct AuthSpecBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub authentication: Vec<Box<dyn AuthenticateLegTrait<TState, TCredentials, TTransports>>>,
    get_message_leg_transport: fn(transports: &TTransports) -> &TTransport,
    message_leg_timeout: Duration,
    message_leg_gather_signatures: Vec<
        Box<
            dyn Fn(&TState, &TCredentials, &mut TMessage) -> StockTrekResult<()>
                + Send
                + Sync
                + 'static,
        >,
    >,
    _phantom_repy: PhantomData<TReply>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    AuthSpecBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        get_message_leg_transport: fn(transports: &TTransports) -> &TTransport,
        message_leg_timeout: Duration,
    ) -> Self {
        Self {
            authentication: Vec::new(),
            get_message_leg_transport,
            message_leg_timeout,
            message_leg_gather_signatures: Vec::new(),
            _phantom_repy: PhantomData,
        }
    }
    pub fn begin_authentication_leg<TAuthTransport, TAuthMessage, TAuthReply>(
        self,
        get_transport: fn(transports: &TTransports) -> &TAuthTransport,
        timeout: Duration,
    ) -> AuthenticateLegBuilder<
        TState,
        TCredentials,
        TTransports,
        TTransport,
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
        AuthenticateLegBuilder::new(self, get_transport, timeout)
    }
    pub fn build_spec(&mut self) -> AuthSpec<TState, TCredentials, TTransports, TMessage, TReply> {
        let message_leg = Box::new(MessageLegGeneric::<
            TState,
            TCredentials,
            TTransports,
            TTransport,
            TMessage,
            TReply,
        >::new(
            self.get_message_leg_transport,
            self.message_leg_timeout,
            std::mem::take(&mut self.message_leg_gather_signatures),
        ));
        AuthSpec::<TState, TCredentials, TTransports, TMessage, TReply>::new(
            std::mem::take(&mut self.authentication),
            message_leg,
        )
    }
}
