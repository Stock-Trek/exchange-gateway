use crate::{credentials::credential::Credential, destroy::Destroy, values::get_value::GetValue};
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct GetCredential<TCredentials, TGetCredential>
where
    TCredentials: Destroy,
    TGetCredential: Fn(&TCredentials) -> Box<dyn Credential>,
{
    get_credential: TGetCredential,
    _phantom_credentials: PhantomData<TCredentials>,
}

impl<TCredentials, TGetCredential> GetCredential<TCredentials, TGetCredential>
where
    TCredentials: Destroy,
    TGetCredential: Fn(&TCredentials) -> Box<dyn Credential>,
{
    pub fn new(get_credential: TGetCredential) -> Self {
        Self {
            get_credential,
            _phantom_credentials: PhantomData,
        }
    }
}

impl<TState, TCredentials, TGetCredential> GetValue<Vec<u8>, TState, TCredentials>
    for GetCredential<TCredentials, TGetCredential>
where
    TCredentials: Destroy,
    TGetCredential: Fn(&TCredentials) -> Box<dyn Credential>,
{
    fn get(&self, _state: &TState, credentials: &TCredentials) -> StockTrekResult<Vec<u8>> {
        let credential = (self.get_credential)(credentials);
        Ok(credential.credential())
    }
}
