use crate::{
    error::{EGError, EGResult},
    sign::encrypt::data_signer::DataSignerTrait,
};
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretSlice};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub(crate) struct HmacSha256Signer {
    hmac_slice: SecretSlice<u8>,
}

impl HmacSha256Signer {
    pub fn new(hmac_slice: SecretSlice<u8>) -> Self {
        Self { hmac_slice }
    }
}

impl DataSignerTrait for HmacSha256Signer {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>> {
        let mut mac = HmacSha256::new_from_slice(self.hmac_slice.expose_secret())
            .map_err(|e| EGError::CryptoKey(format!("HMAC-SHA256 key error: {e}")))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use crate::sign::encrypt::{
        data_signer::DataSignerTrait,
        hmac_sha256::{HmacSha256, HmacSha256Signer},
    };
    use hmac::Mac;
    use secrecy::SecretSlice;

    #[test]
    fn signing() {
        let key = vec![1, 2, 3, 4, 5];
        let signer = HmacSha256Signer::new(SecretSlice::<u8>::from(key.clone()));
        let msg = b"hello world";
        let sig = signer.sign(msg).unwrap();
        assert!(!sig.is_empty());
        let mut mac = HmacSha256::new_from_slice(&key).unwrap();
        mac.update(msg);
        mac.verify_slice(&sig).unwrap();
    }
}
