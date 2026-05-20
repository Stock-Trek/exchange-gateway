use crate::{
    destroy::Destroy,
    values::{get_value::GetValue, pack_value::PackValue},
};
use stock_trek::error::result::StockTrekResult;

pub type GatherValue<TState, TCredentials, TMessage> =
    Box<dyn GatherValueTrait<TState, TCredentials, TMessage>>;

pub trait GatherValueTrait<TState, TCredentials, TMessage>: Send + Sync {
    fn gather(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()>;
}

pub struct GatherValueGeneric<TValue, TState, TCredentials, TMessage>
where
    TValue: Clone,
    TCredentials: Destroy,
{
    get_value: GetValue<TValue, TState, TCredentials>,
    pack_value: PackValue<TValue, TMessage>,
}

impl<TValue, TState, TCredentials, TMessage>
    GatherValueGeneric<TValue, TState, TCredentials, TMessage>
where
    TValue: Clone,
    TCredentials: Destroy,
{
    pub fn new(
        get_value: GetValue<TValue, TState, TCredentials>,
        pack_value: PackValue<TValue, TMessage>,
    ) -> Self {
        Self {
            get_value,
            pack_value,
        }
    }
}

impl<TValue, TState, TCredentials, TMessage> GatherValueTrait<TState, TCredentials, TMessage>
    for GatherValueGeneric<TValue, TState, TCredentials, TMessage>
where
    TValue: Clone,
    TCredentials: Destroy,
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
