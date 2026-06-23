use crate::{error::EGResult, sign::encrypt::data_signer::DataSignerTrait};
use ed25519_compact::SecretKey;

pub struct Ed25519Signer {
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
    use ed25519_compact::{KeyPair, Seed};

    #[test]
    fn ed25519() {
        let seed = Seed::generate();
        let key_pair = KeyPair::from_seed(seed);
        let msg = b"hello world";
        let sig = key_pair.sk.sign(msg, None);
        assert_eq!(sig.len(), 64);
        key_pair.pk.verify(msg, &sig).unwrap();
    }
}
