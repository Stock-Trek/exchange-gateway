use crate::{
    authenticate_leg::AuthenticateLegGeneric, build::auth_spec_builder::AuthSpecBuilder,
    destroy::Destroy, transport::transport::Transport,
};
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

pub struct AuthenticateLegBuilder<
    TState,
    TCredentials,
    TTransports,
    TTransport,
    TMessage,
    TReply,
    TAuthTransport,
    TAuthMessage,
    TAuthReply,
> where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
    TAuthTransport: Transport<TAuthMessage, TAuthReply> + Send + Sync + 'static,
{
    auth_spec_builder:
        Option<AuthSpecBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply>>,
    get_transport: fn(transports: &TTransports) -> &TAuthTransport,
    timeout: Duration,
    gather_values:
        Vec<Box<dyn Fn(&TState, &TCredentials, &mut TAuthMessage) + Send + Sync + 'static>>,
    store_values:
        Vec<Box<dyn Fn(&TAuthReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>>,
}

impl<
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
    AuthenticateLegBuilder<
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
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
    TAuthTransport: Transport<TAuthMessage, TAuthReply> + Send + Sync + 'static,
    TAuthMessage: Send + Sync + 'static,
    TAuthReply: Send + Sync + 'static,
{
    pub fn new(
        auth_spec_builder: AuthSpecBuilder<
            TState,
            TCredentials,
            TTransports,
            TTransport,
            TMessage,
            TReply,
        >,
        get_transport: fn(transports: &TTransports) -> &TAuthTransport,
        timeout: Duration,
    ) -> Self {
        Self {
            auth_spec_builder: Some(auth_spec_builder),
            get_transport,
            timeout,
            gather_values: Vec::new(),
            store_values: Vec::new(),
        }
    }
    pub fn gather_value<TValue>(
        &mut self,
        get_value: fn(state: &TState, credentials: &TCredentials) -> TValue,
        pack_value: fn(message: &mut TAuthMessage, value: &TValue) -> (),
    ) -> &mut Self
    where
        TValue: Send + Sync + 'static,
    {
        let gather_value =
            move |state: &TState, credentials: &TCredentials, message: &mut TAuthMessage| {
                let value = get_value(state, credentials);
                pack_value(message, &value)
            };
        self.gather_values.push(Box::new(gather_value));
        self
    }
    pub fn store_value<TValue>(
        &mut self,
        unpack_value: fn(reply: &TAuthReply) -> StockTrekResult<TValue>,
        set_value: fn(state: &mut TState, value: &TValue),
    ) -> &mut Self
    where
        TValue: Send + Sync + 'static,
    {
        let store_value = move |reply: &TAuthReply, state: &mut TState| -> StockTrekResult<()> {
            let value = unpack_value(reply)?;
            set_value(state, &value);
            Ok(())
        };
        self.store_values.push(Box::new(store_value));
        self
    }
    pub fn build_leg(
        &mut self,
    ) -> AuthSpecBuilder<TState, TCredentials, TTransports, TTransport, TMessage, TReply> {
        let authenticate_leg = AuthenticateLegGeneric::<
            TState,
            TCredentials,
            TTransports,
            TAuthTransport,
            TAuthMessage,
            TAuthReply,
        >::new(
            self.get_transport,
            self.timeout,
            std::mem::take(&mut self.gather_values),
            std::mem::take(&mut self.store_values),
        );
        let mut builder = self
            .auth_spec_builder
            .take()
            .expect("Already built AuthLegBuilder");
        builder.authentication.push(Box::new(authenticate_leg));
        builder
    }
}
