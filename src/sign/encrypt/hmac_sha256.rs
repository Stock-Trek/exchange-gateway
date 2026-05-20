use crate::sign::encrypt::data_signer::DataSignerTrait;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

type HmacSha256 = Hmac<Sha256>;

pub struct HmacSha256Signer;

impl DataSignerTrait for HmacSha256Signer {
    fn sign(&self, data: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        let mut mac = HmacSha256::new_from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!("HMAC-SHA256 key error: {e}")))
        })?;
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

    #[test]
    fn hmac_sha256() {
        let signer = HmacSha256Signer;
        let key = b"my-secret-key";
        let msg = b"hello world";
        let sig = signer.sign(msg, key).unwrap();
        assert!(!sig.is_empty());
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.verify_slice(&sig).unwrap();
    }
}
