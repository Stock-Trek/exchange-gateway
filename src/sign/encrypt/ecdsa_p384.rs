use crate::sign::encrypt::data_signer::DataSignerTrait;
use p384::ecdsa::{Signature, SigningKey, signature::Signer};
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub struct EcdsaP384Signer;

impl DataSignerTrait for EcdsaP384Signer {
    fn sign(&self, data: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        let signing_key = SigningKey::from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!("ECDSA P-384 key error: {e}")))
        })?;
        let signature: Signature = signing_key.sign(data);
        Ok(signature.to_der().to_bytes().to_vec())
    }
}
