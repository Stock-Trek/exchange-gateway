use crate::sign::encrypt::data_signer::DataSignerTrait;
use ed25519_dalek::Signer;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub struct Ed25519Signer;

impl DataSignerTrait for Ed25519Signer {
    fn sign(&self, data: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        let key_bytes: [u8; 32] = key.try_into().map_err(|_| {
            StockTrekError::General(GeneralError::Message(
                "Ed25519 key must be exactly 32 bytes".to_string(),
            ))
        })?;
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_bytes);
        let signature = signing_key.sign(data);
        Ok(signature.to_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use crate::sign::encrypt::{data_signer::DataSignerTrait, ed25519::Ed25519Signer};
    use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
    use signature::Verifier;

    #[test]
    fn ed25519() {
        let key_bytes: [u8; 32] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c,
            0x1d, 0x1e, 0x1f, 0x20,
        ];
        let signer = Ed25519Signer;
        let msg = b"hello world";
        let sig = signer.sign(msg, &key_bytes).unwrap();
        assert_eq!(sig.len(), 64);
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = VerifyingKey::from(&signing_key);
        let sig_array: [u8; 64] = sig.as_slice().try_into().unwrap();
        let ed_sig = Signature::from_bytes(&sig_array);
        verifying_key.verify(msg, &ed_sig).unwrap();
    }
}
