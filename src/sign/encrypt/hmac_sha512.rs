use crate::{
    error::{EGError, EGResult},
    sign::encrypt::data_signer::DataSignerTrait,
};
use hmac::{Hmac, Mac};
use secrecy::{ExposeSecret, SecretSlice};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

pub(crate) struct HmacSha512Signer {
    hmac_slice: SecretSlice<u8>,
}

impl HmacSha512Signer {
    pub fn new(hmac_slice: SecretSlice<u8>) -> Self {
        Self { hmac_slice }
    }
}

impl DataSignerTrait for HmacSha512Signer {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>> {
        let mut mac = HmacSha512::new_from_slice(self.hmac_slice.expose_secret())
            .map_err(|e| EGError::CryptoKey(format!("HMAC-SHA512 key error: {e}")))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

#[cfg(test)]
mod tests {
    use crate::sign::encrypt::{
        data_signer::DataSignerTrait,
        hmac_sha512::{HmacSha512, HmacSha512Signer},
    };
    use hmac::Mac;
    use secrecy::SecretSlice;

    #[test]
    fn signature_matches_independently_computed_hmac_sha512() {
        let key = vec![1, 2, 3, 4, 5];
        let signer = HmacSha512Signer::new(SecretSlice::<u8>::from(key.clone()));
        let msg = b"hello world";
        let sig = signer.sign(msg).unwrap();
        assert!(!sig.is_empty());
        let mut mac = HmacSha512::new_from_slice(&key).unwrap();
        mac.update(msg);
        mac.verify_slice(&sig).unwrap();
    }
}
