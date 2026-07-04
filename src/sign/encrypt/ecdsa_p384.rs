use crate::{error::EGResult, sign::encrypt::data_signer::DataSignerTrait};
use p384::ecdsa::{Signature, SigningKey, signature::Signer};

pub(crate) struct EcdsaP384Signer {
    signing_key: SigningKey,
}

impl EcdsaP384Signer {
    pub fn new(signing_key: SigningKey) -> Self {
        Self { signing_key }
    }
}

impl DataSignerTrait for EcdsaP384Signer {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>> {
        let signature: Signature = self.signing_key.sign(data);
        Ok(signature.to_der().to_bytes().to_vec())
    }
}
