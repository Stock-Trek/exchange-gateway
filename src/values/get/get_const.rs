use crate::{credentials::credential::Credential, values::get_value::GetValue};
use stock_trek::error::result::StockTrekResult;

pub struct GetConst<TValue>
where
    TValue: Clone,
{
    value: TValue,
}

impl<TValue> GetConst<TValue>
where
    TValue: Clone,
{
    pub fn new(value: TValue) -> Self {
        Self { value }
    }
}

impl<TValue, TState, TCredentials> GetValue<TValue, TState, TCredentials> for GetConst<TValue>
where
    TValue: Clone,
    TCredentials: Credential,
{
    fn get(&self, _state: &TState, _credential: &TCredentials) -> StockTrekResult<TValue> {
        Ok(self.value.clone())
    }
}
