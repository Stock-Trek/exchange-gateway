use crate::{credentials::credential::Credential, destroy::Destroy, values::get_value::GetValue};
use stock_trek::error::result::StockTrekResult;

pub struct GetCredential<TCredentials>
where
    TCredentials: Destroy,
{
    get_credential: fn(&TCredentials) -> Box<dyn Credential>,
}

impl<TCredentials> GetCredential<TCredentials>
where
    TCredentials: Destroy,
{
    pub fn new(get_credential: fn(&TCredentials) -> Box<dyn Credential>) -> Self {
        Self { get_credential }
    }
}

impl<TState, TCredentials> GetValue<Vec<u8>, TState, TCredentials> for GetCredential<TCredentials>
where
    TCredentials: Destroy,
{
    fn get(&self, _state: &TState, credentials: &TCredentials) -> StockTrekResult<Vec<u8>> {
        let credential = (self.get_credential)(credentials);
        Ok(credential.credential())
    }
}
