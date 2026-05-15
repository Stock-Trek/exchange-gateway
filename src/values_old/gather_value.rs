use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{get_value::GetValue, pack_value::PackValue},
};
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub trait GatherValue<TState, TCredentials, TMessage>
where
    TCredentials: Destroy,
{
    fn gather(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()>;
}

pub struct GatherValueWrapper<
    TValue,
    TState,
    TCredentials,
    TTransport,
    TMessagePart,
    TMessage,
    TReply,
    TGetValue,
    TPackValue,
> where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
    TGetValue: GetValue<TValue, TState, TCredentials>,
    TPackValue: PackValue<TValue, TMessagePart, TMessage>,
{
    get_value: TGetValue,
    pack_value: TPackValue,
    _phantom_value: PhantomData<TValue>,
    _phantom_state: PhantomData<TState>,
    _phantom_credentials: PhantomData<TCredentials>,
    _phantom_transport: PhantomData<TTransport>,
    _phantom_message_part: PhantomData<TMessagePart>,
    _phantom_message: PhantomData<TMessage>,
    _phantom_reply: PhantomData<TReply>,
}

impl<
    TValue,
    TState,
    TCredentials,
    TTransport,
    TMessagePart,
    TMessage,
    TReply,
    TGetValue,
    TPackValue,
>
    GatherValueWrapper<
        TValue,
        TState,
        TCredentials,
        TTransport,
        TMessagePart,
        TMessage,
        TReply,
        TGetValue,
        TPackValue,
    >
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
    TGetValue: GetValue<TValue, TState, TCredentials>,
    TPackValue: PackValue<TValue, TMessagePart, TMessage>,
{
    pub fn new(get_value: TGetValue, pack_value: TPackValue) -> Self {
        Self {
            get_value,
            pack_value,
            _phantom_value: PhantomData,
            _phantom_state: PhantomData,
            _phantom_credentials: PhantomData,
            _phantom_transport: PhantomData,
            _phantom_message_part: PhantomData,
            _phantom_message: PhantomData,
            _phantom_reply: PhantomData,
        }
    }
}

impl<
    TValue,
    TState,
    TCredentials,
    TTransport,
    TMessagePart,
    TMessage,
    TReply,
    TGetValue,
    TPackValue,
> GatherValue<TState, TCredentials, TMessage>
    for GatherValueWrapper<
        TValue,
        TState,
        TCredentials,
        TTransport,
        TMessagePart,
        TMessage,
        TReply,
        TGetValue,
        TPackValue,
    >
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
    TGetValue: GetValue<TValue, TState, TCredentials>,
    TPackValue: PackValue<TValue, TMessagePart, TMessage>,
{
    fn gather(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()> {
        let value = self.get_value.get(state, credentials)?;
        self.pack_value.pack(message, &value)?;
        Ok(())
    }
}
