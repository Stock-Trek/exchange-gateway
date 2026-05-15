use crate::{
    auth_spec::{AuthLeg, AuthLegTrait, AuthSpec},
    destroy::Destroy,
    transport::transport::Transport,
};
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct AuthSpecBuilder<TState, TCredentials, TTransports>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
{
    legs: Vec<Box<dyn AuthLegTrait<TState, TCredentials, TTransports>>>,
}

impl<TState, TCredentials, TTransports> AuthSpecBuilder<TState, TCredentials, TTransports>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self { legs: Vec::new() }
    }
    pub fn begin_leg<TTransport, TMessagePart, TMessage, TReply>(
        mut self,
        get_transport: fn(transports: &TTransports) -> &TTransport,
    ) -> AuthLegBuilder<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
    where
        TTransport: Transport<TMessagePart, TMessage, TReply> + Send + Sync + 'static,
        TMessagePart: Send + Sync + 'static,
        TMessage: Send + Sync + 'static,
        TReply: Send + Sync + 'static,
    {
        let legs = std::mem::take(&mut self.legs);
        let builder = AuthSpecBuilder { legs };
        AuthLegBuilder::new(builder, get_transport)
    }
    pub fn build_spec(&mut self) -> AuthSpec<TState, TCredentials, TTransports> {
        AuthSpec::<TState, TCredentials, TTransports>::new(std::mem::take(&mut self.legs))
    }
}

pub struct AuthLegBuilder<
    TState,
    TCredentials,
    TTransports,
    TTransport,
    TMessagePart,
    TMessage,
    TReply,
> where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransport: Transport<TMessagePart, TMessage, TReply> + Send + Sync + 'static,
{
    auth_spec_builder: Option<AuthSpecBuilder<TState, TCredentials, TTransports>>,
    get_transport: fn(transports: &TTransports) -> &TTransport,
    gather_values: Vec<Box<dyn Fn(&TState, &TCredentials, &mut TMessage) + Send + Sync + 'static>>,
    store_values:
        Vec<Box<dyn Fn(&TReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>>,
    _phantom_message_part: PhantomData<TMessagePart>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
    AuthLegBuilder<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessagePart, TMessage, TReply> + Send + Sync + 'static,
    TMessagePart: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    fn new(
        auth_spec_builder: AuthSpecBuilder<TState, TCredentials, TTransports>,
        get_transport: fn(transports: &TTransports) -> &TTransport,
    ) -> Self {
        Self {
            auth_spec_builder: Some(auth_spec_builder),
            get_transport,
            gather_values: Vec::new(),
            store_values: Vec::new(),
            _phantom_message_part: PhantomData,
        }
    }
    pub fn gather_value<TValue>(
        &mut self,
        get_value: fn(state: &TState, credentials: &TCredentials) -> TValue,
        pack_value: fn(message: &mut TMessage, value: &TValue) -> (),
    ) -> &mut Self
    where
        TValue: Send + Sync + 'static,
    {
        let gather_value =
            move |state: &TState, credentials: &TCredentials, message: &mut TMessage| {
                let value = get_value(state, credentials);
                pack_value(message, &value)
            };
        self.gather_values.push(Box::new(gather_value));
        self
    }
    pub fn store_value<TValue>(
        &mut self,
        unpack_value: fn(reply: &TReply) -> StockTrekResult<TValue>,
        set_value: fn(state: &mut TState, value: &TValue),
    ) -> &mut Self
    where
        TValue: Send + Sync + 'static,
    {
        let store_value = move |reply: &TReply, state: &mut TState| -> StockTrekResult<()> {
            let value = unpack_value(reply)?;
            set_value(state, &value);
            Ok(())
        };
        self.store_values.push(Box::new(store_value));
        self
    }
    pub fn build_leg(&mut self) -> AuthSpecBuilder<TState, TCredentials, TTransports> {
        let auth_leg = AuthLeg::<
            TState,
            TCredentials,
            TTransports,
            TTransport,
            TMessagePart,
            TMessage,
            TReply,
        >::new(
            self.get_transport,
            std::mem::take(&mut self.gather_values),
            std::mem::take(&mut self.store_values),
        );
        let mut builder = self
            .auth_spec_builder
            .take()
            .expect("Already built AuthLegBuilder");
        builder.legs.push(Box::new(auth_leg));
        builder
    }
}
