use crate::{
    credentials::credential::Credential,
    values::{
        get_value::GetValue,
        hash_encoder::{Encoding, HashAlgorithm, HasherEncoder},
    },
};
use stock_trek::error::result::StockTrekResult;

pub struct GetHash<TState, TCredentials, THashableValue>
where
    TCredentials: Credential,
{
    get_hashable_value: Box<dyn GetValue<THashableValue, TState, TCredentials>>,
    hash_encoder: HasherEncoder<THashableValue>,
}

impl<TState, TCredentials, THashableValue> GetHash<TState, TCredentials, THashableValue>
where
    TCredentials: Credential,
{
    pub fn new(
        get_hashable_value: Box<dyn GetValue<THashableValue, TState, TCredentials>>,
        to_bytes: fn(&THashableValue) -> &Vec<u8>,
        hash_algorithm: HashAlgorithm,
        encoding: Encoding,
    ) -> Self {
        Self {
            get_hashable_value,
            hash_encoder: HasherEncoder::new(to_bytes, hash_algorithm, encoding),
        }
    }
}

impl<TState, TCredentials, THashableValue> GetValue<String, TState, TCredentials>
    for GetHash<TState, TCredentials, THashableValue>
where
    TCredentials: Credential,
{
    fn get(&self, state: &TState, credential: &TCredentials) -> StockTrekResult<String> {
        let hashable_value = self.get_hashable_value.get(state, credential)?;
        let encoded = self.hash_encoder.hash_encode(&hashable_value)?;
        Ok(encoded)
    }
}
