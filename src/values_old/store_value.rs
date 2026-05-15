use crate::values::{set_value::SetValue, unpack_value::UnpackValue};
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub trait StoreValue<TReply, TState> {
    fn store_value(&self, reply: &TReply, state: &mut TState) -> StockTrekResult<()>;
}

pub struct StoreValueWrapper<TValue, TReply, TUnpackValue, TSetValue, TState>
where
    TValue: Clone,
    TUnpackValue: UnpackValue<TValue, TReply>,
    TSetValue: SetValue<TValue, TState>,
{
    unpack_value: TUnpackValue,
    set_value: TSetValue,
    _phantom_value: PhantomData<TValue>,
    _phantom_state: PhantomData<TState>,
    _phantom_reply: PhantomData<TReply>,
}

impl<TValue, TReply, TUnpackValue, TSetValue, TState>
    StoreValueWrapper<TValue, TReply, TUnpackValue, TSetValue, TState>
where
    TValue: Clone,
    TUnpackValue: UnpackValue<TValue, TReply>,
    TSetValue: SetValue<TValue, TState>,
{
    pub fn new(unpack_value: TUnpackValue, set_value: TSetValue) -> Self {
        Self {
            unpack_value,
            set_value,
            _phantom_value: PhantomData,
            _phantom_state: PhantomData,
            _phantom_reply: PhantomData,
        }
    }
}

impl<TValue, TReply, TUnpackValue, TSetValue, TState> StoreValue<TReply, TState>
    for StoreValueWrapper<TValue, TReply, TUnpackValue, TSetValue, TState>
where
    TValue: Clone,
    TUnpackValue: UnpackValue<TValue, TReply>,
    TSetValue: SetValue<TValue, TState>,
{
    fn store_value(&self, reply: &TReply, state: &mut TState) -> StockTrekResult<()> {
        let value = self.unpack_value.unpack(reply)?;
        self.set_value.set(state, &value)?;
        Ok(())
    }
}
