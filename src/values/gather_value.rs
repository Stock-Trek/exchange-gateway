use std::marker::PhantomData;

use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{get_value::GetValue, pack_value::PackValue},
};
use stock_trek::error::result::StockTrekResult;

pub struct GatherValue<TValue, TState, TCredentials, TTransport, TMessage, TReply>
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessage, TReply>,
{
    get_value: GetValue<TValue, TState, TCredentials>,
    pack_value: PackValue<TValue, TMessage>,
    _phantom_transport: PhantomData<TTransport>,
    _phantom_reply: PhantomData<TReply>,
}

impl<TValue, TState, TCredentials, TTransport, TMessage, TReply>
    GatherValue<TValue, TState, TCredentials, TTransport, TMessage, TReply>
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessage, TReply>,
{
    pub fn new(
        get_value: GetValue<TValue, TState, TCredentials>,
        pack_value: PackValue<TValue, TMessage>,
    ) -> Self {
        Self {
            get_value,
            pack_value,
            _phantom_transport: PhantomData,
            _phantom_reply: PhantomData,
        }
    }
}

impl<TValue, TState, TCredentials, TTransport, TMessage, TReply>
    GatherValue<TValue, TState, TCredentials, TTransport, TMessage, TReply>
where
    TValue: Clone,
    TCredentials: Destroy,
    TTransport: Transport<TMessage, TReply>,
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
