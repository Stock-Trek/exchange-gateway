use crate::sign::encrypt::{
    ecdsa_p256::EcdsaP256Signer, ecdsa_p384::EcdsaP384Signer, ed25519::Ed25519Signer,
    hmac_sha256::HmacSha256Signer, hmac_sha512::HmacSha512Signer,
    signing_algorithm::SigningAlgorithm,
};
use stock_trek::error::result::StockTrekResult;

pub type DataSigner = Box<dyn DataSignerTrait>;

pub trait DataSignerTrait: Send + Sync {
    fn sign(&self, data: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>>;
}

impl From<SigningAlgorithm> for DataSigner {
    fn from(value: SigningAlgorithm) -> Self {
        match value {
            SigningAlgorithm::HmacSha256 => Box::new(HmacSha256Signer),
            SigningAlgorithm::HmacSha512 => Box::new(HmacSha512Signer),
            SigningAlgorithm::EcdsaP256 => Box::new(EcdsaP256Signer),
            SigningAlgorithm::EcdsaP384 => Box::new(EcdsaP384Signer),
            SigningAlgorithm::Ed25519 => Box::new(Ed25519Signer),
        }
    }
}
