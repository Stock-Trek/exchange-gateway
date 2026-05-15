use crate::values::{set_value::SetValue, unpack_value::UnpackValue};
use stock_trek::error::result::StockTrekResult;

pub struct StoreValue<TValue, TReply, TState>
where
    TValue: Clone,
{
    unpack_value: UnpackValue<TValue, TReply>,
    set_value: SetValue<TValue, TState>,
}

impl<TValue, TReply, TState> StoreValue<TValue, TReply, TState>
where
    TValue: Clone,
{
    pub fn new(
        unpack_value: UnpackValue<TValue, TReply>,
        set_value: SetValue<TValue, TState>,
    ) -> Self {
        Self {
            unpack_value,
            set_value,
        }
    }
}

impl<TValue, TReply, TState> StoreValue<TValue, TReply, TState>
where
    TValue: Clone,
{
    fn store_value(&self, reply: &TReply, state: &mut TState) -> StockTrekResult<()> {
        let value = (self.unpack_value)(reply)?;
        (self.set_value)(state, &value)?;
        Ok(())
    }
}
