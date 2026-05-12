use crate::values::set_value::SetValue;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct SetStateValue<TState, TSetter, TValue>
where
    TSetter: Fn(&TState, TValue),
    TValue: Clone,
{
    setter: TSetter,
    _phantom_state: PhantomData<TState>,
    _phantom_value: PhantomData<TValue>,
}

impl<TState, TSetter, TValue> SetStateValue<TState, TSetter, TValue>
where
    TSetter: Fn(&TState, TValue),
    TValue: Clone,
{
    pub fn new(setter: TSetter) -> Self {
        Self {
            setter,
            _phantom_state: PhantomData,
            _phantom_value: PhantomData,
        }
    }
}

impl<TState, TSetter, TValue> SetValue<TValue, TState> for SetStateValue<TState, TSetter, TValue>
where
    TSetter: Fn(&TState, TValue),
    TValue: Clone,
{
    fn set(&self, state: &mut TState, value: &TValue) -> StockTrekResult<()> {
        Ok((self.setter)(state, value.clone()))
    }
}
