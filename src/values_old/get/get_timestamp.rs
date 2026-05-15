use crate::{credentials::credential::Credential, values::get_value::GetValue};
use chrono::Utc;
use stock_trek::error::result::StockTrekResult;

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GetTimestamp {
    Millis,
    Seconds,
}

impl<TState, TCredentials> GetValue<i64, TState, TCredentials> for GetTimestamp
where
    TCredentials: Credential,
{
    fn get(&self, _state: &TState, _credential: &TCredentials) -> StockTrekResult<i64> {
        let timestamp = match self {
            Self::Millis => Utc::now().timestamp_millis(),
            Self::Seconds => Utc::now().timestamp(),
        };
        Ok(timestamp)
    }
}
