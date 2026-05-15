use std::marker::PhantomData;

use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{get_value::GetValue, pack_value::PackValue},
};
use stock_trek::error::result::StockTrekResult;

pub struct GatherValue<TValue, TState, TCredentials, TTransport, TMessagePart, TMessage, TReply>
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
{
    get_value: GetValue<TValue, TState, TCredentials>,
    pack_value: PackValue<TValue, TMessage>,
    _phantom_transport: PhantomData<TTransport>,
    _phantom_message_part: PhantomData<TMessagePart>,
    _phantom_reply: PhantomData<TReply>,
}

impl<TValue, TState, TCredentials, TTransport, TMessagePart, TMessage, TReply>
    GatherValue<TValue, TState, TCredentials, TTransport, TMessagePart, TMessage, TReply>
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
{
    pub fn new(
        get_value: GetValue<TValue, TState, TCredentials>,
        pack_value: PackValue<TValue, TMessage>,
    ) -> Self {
        Self {
            get_value,
            pack_value,
            _phantom_transport: PhantomData,
            _phantom_message_part: PhantomData,
            _phantom_reply: PhantomData,
        }
    }
}

impl<TValue, TState, TCredentials, TTransport, TMessagePart, TMessage, TReply>
    GatherValue<TValue, TState, TCredentials, TTransport, TMessagePart, TMessage, TReply>
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessagePart, TMessage, TReply>,
{
    fn gather(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()> {
        let value = (self.get_value)(state, credentials)?;
        (self.pack_value)(message, &value)?;
        Ok(())
    }
}
