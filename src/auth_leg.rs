use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{gather_value::GatherValue, store_value::StoreValue},
};
use async_trait::async_trait;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

#[async_trait]
pub trait AuthLeg<TState, TCredentials, TTransports>
where
    TCredentials: Destroy,
{
    async fn do_leg(
        &self,
        state: &mut TState,
        credentials: &TCredentials,
        transports: &TTransports,
    ) -> StockTrekResult<()>;
}

pub struct AuthLegWrapper<
    TCredentials,
    TTransports,
    TState,
    TTransport,
    TMessagePart,
    TMessage,
    TReply,
    TGetTransport,
> where
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
    TGetTransport: Fn(&TTransports) -> TTransport,
{
    gather_values: Vec<Box<dyn GatherValue<TState, TCredentials, TMessage>>>,
    get_transport: TGetTransport,
    store_values: Vec<Box<dyn StoreValue<TReply, TState>>>,
    _phantom_credentials: PhantomData<TCredentials>,
    _phantom_transports: PhantomData<TTransports>,
    _phantom_state: PhantomData<TState>,
    _phantom_transport: PhantomData<TTransport>,
    _phantom_message_part: PhantomData<TMessagePart>,
    _phantom_message: PhantomData<TMessage>,
    _phantom_reply: PhantomData<TReply>,
}

impl<TCredentials, TTransports, TState, TTransport, TMessagePart, TMessage, TReply, TGetTransport>
    AuthLegWrapper<
        TCredentials,
        TTransports,
        TState,
        TTransport,
        TMessagePart,
        TMessage,
        TReply,
        TGetTransport,
    >
where
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
    TGetTransport: Fn(&TTransports) -> TTransport,
{
    pub fn new(get_transport: TGetTransport) -> Self {
        Self {
            gather_values: Vec::new(),
            get_transport,
            store_values: Vec::new(),
            _phantom_credentials: PhantomData,
            _phantom_transports: PhantomData,
            _phantom_state: PhantomData,
            _phantom_transport: PhantomData,
            _phantom_message_part: PhantomData,
            _phantom_message: PhantomData,
            _phantom_reply: PhantomData,
        }
    }
    pub fn gather_value(&mut self) -> &mut Self {
        self
    }
    pub fn store_value(&mut self) -> &mut Self {
        self
    }
}

impl<TCredentials, TTransports, TState, TTransport, TMessagePart, TMessage, TReply, TGetTransport>
    AuthLeg<TState, TCredentials, TTransports>
    for AuthLegWrapper<
        TCredentials,
        TTransports,
        TState,
        TTransport,
        TMessagePart,
        TMessage,
        TReply,
        TGetTransport,
    >
where
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
    TGetTransport: Fn(&TTransports) -> TTransport,
{
    async fn do_leg(
        &self,
        state: &mut TState,
        credentials: &TCredentials,
        transports: &TTransports,
    ) -> StockTrekResult<()> {
        let transport = (self.get_transport)(transports);
        let mut message = transport.new_message()?;
        for gather_value in &self.gather_values {
            gather_value.gather(state, credentials, &mut message);
        }
        let reply = transport.send_and_wait_for_reply(message).await?;
        for store_value in &self.store_values {
            store_value.store_value(&reply, state);
        }
        Ok(())
    }
}
