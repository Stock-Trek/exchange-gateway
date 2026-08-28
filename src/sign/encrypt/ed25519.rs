use crate::{error::EGResult, sign::encrypt::data_signer::DataSignerTrait};
use ed25519_compact::SecretKey;

pub(crate) struct Ed25519Signer {
    secret_key: SecretKey,
}

impl Ed25519Signer {
    pub fn new(secret_key: SecretKey) -> Self {
        Self { secret_key }
    }
}

impl DataSignerTrait for Ed25519Signer {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>> {
        let signature = self.secret_key.sign(data, None);
        Ok(signature.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use crate::sign::encrypt::{data_signer::DataSignerTrait, ed25519::Ed25519Signer};
    use ed25519_compact::{KeyPair, Seed, Signature};

    #[test]
    fn signing() {
        let key_pair = KeyPair::from_seed(Seed::generate());
        let signer = Ed25519Signer::new(key_pair.sk);
        let msg = b"hello world";
        let sig = signer.sign(msg).unwrap();
        assert_eq!(sig.len(), 64);
        let parsed = Signature::from_slice(&sig).unwrap();
        key_pair.pk.verify(msg, &parsed).unwrap();
    }
}
