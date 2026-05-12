use crate::{credentials::credential::Credential, values::get_value::GetValue};
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct GetStateValue<TState, TGetter, TValue>
where
    TGetter: Fn(&TState) -> TValue,
{
    getter: TGetter,
    _phantom: PhantomData<TState>,
}

impl<TState, TGetter, TValue> GetStateValue<TState, TGetter, TValue>
where
    TGetter: Fn(&TState) -> TValue,
{
    pub fn new(getter: TGetter) -> Self {
        Self {
            getter,
            _phantom: PhantomData,
        }
    }
}

impl<TState, TGetter, TValue, TCredentials> GetValue<TValue, TState, TCredentials>
    for GetStateValue<TState, TGetter, TValue>
where
    TGetter: Fn(&TState) -> TValue,
    TCredentials: Credential,
{
    fn get(&self, state: &TState, _credential: &TCredentials) -> StockTrekResult<TValue> {
        Ok((self.getter)(state))
    }
}
