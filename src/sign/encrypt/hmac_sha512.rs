use crate::sign::encrypt::data_signer::DataSignerTrait;
use hmac::{Hmac, Mac};
use sha2::Sha512;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

type HmacSha512 = Hmac<Sha512>;

pub struct HmacSha512Signer;

impl DataSignerTrait for HmacSha512Signer {
    fn sign(&self, data: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        let mut mac = HmacSha512::new_from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!("HMAC-SHA512 key error: {e}")))
        })?;
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

    #[test]
    fn hmac_sha512() {
        let signer = HmacSha512Signer;
        let key = b"my-secret-key";
        let msg = b"hello world";
        let sig = signer.sign(msg, key).unwrap();
        assert!(!sig.is_empty());
        let mut mac = HmacSha512::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.verify_slice(&sig).unwrap();
    }
}
