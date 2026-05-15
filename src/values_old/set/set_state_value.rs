use crate::values::set_value::SetValue;
use stock_trek::error::result::StockTrekResult;

pub struct SetStateValue<TState, TValue>
where
    TValue: Clone,
{
    setter: fn(&TState, TValue),
}

impl<TState, TValue> SetStateValue<TState, TValue>
where
    TValue: Clone,
{
    pub fn new(setter: fn(&TState, TValue)) -> Self {
        Self { setter }
    }
}

impl<TState, TValue> SetValue<TValue, TState> for SetStateValue<TState, TValue>
where
    TValue: Clone,
{
    fn set(&self, state: &mut TState, value: &TValue) -> StockTrekResult<()> {
        Ok((self.setter)(state, value.clone()))
    }
}
