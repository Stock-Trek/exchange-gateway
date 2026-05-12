use crate::{credentials::credential::Credential, values::get_value::GetValue};
use stock_trek::error::result::StockTrekResult;
use uuid::Uuid;

pub struct GetUuid;

impl<TState, TCredentials> GetValue<String, TState, TCredentials> for GetUuid
where
    TCredentials: Credential,
{
    fn get(&self, _state: &TState, _credential: &TCredentials) -> StockTrekResult<String> {
        let uuid = Uuid::new_v4().to_string();
        Ok(uuid)
    }
}
