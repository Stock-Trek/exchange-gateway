use crate::{credentials::credential::Credential, values::get_value::GetValue};
use stock_trek::error::result::StockTrekResult;

pub struct GetStateValue<TState, TValue> {
    getter: fn(&TState) -> TValue,
}

impl<TState, TValue> GetStateValue<TState, TValue> {
    pub fn new(getter: fn(&TState) -> TValue) -> Self {
        Self { getter }
    }
}

impl<TState, TValue, TCredentials> GetValue<TValue, TState, TCredentials>
    for GetStateValue<TState, TValue>
where
    TCredentials: Credential,
{
    fn get(&self, state: &TState, _credential: &TCredentials) -> StockTrekResult<TValue> {
        Ok((self.getter)(state))
    }
}
