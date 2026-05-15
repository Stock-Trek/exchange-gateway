use base64::{Engine, engine::general_purpose};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};
use stock_trek::error::result::StockTrekResult;
use strum::Display;

pub struct HasherEncoder<THashableValue> {
    to_bytes: fn(&THashableValue) -> &Vec<u8>,
    hash_algorithm: HashAlgorithm,
    encoding: Encoding,
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashAlgorithm {
    Sha256,
    Sha512,
}

#[derive(Debug, Display, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Encoding {
    Hex,
    Base64,
}

impl<THashableValue> HasherEncoder<THashableValue> {
    pub fn new(
        to_bytes: fn(&THashableValue) -> &Vec<u8>,
        hash_algorithm: HashAlgorithm,
        encoding: Encoding,
    ) -> Self {
        Self {
            to_bytes,
            hash_algorithm,
            encoding,
        }
    }
    pub fn hash_encode(&self, hashable_value: &THashableValue) -> StockTrekResult<String> {
        let bytes = (self.to_bytes)(hashable_value);
        let hashed_bytes = match self.hash_algorithm {
            HashAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                hasher.finalize().to_vec()
            }
            HashAlgorithm::Sha512 => {
                let mut hasher = Sha512::new();
                hasher.update(bytes);
                hasher.finalize().to_vec()
            }
        };
        let encoded = match self.encoding {
            Encoding::Hex => hex::encode(hashed_bytes),
            Encoding::Base64 => general_purpose::STANDARD.encode(hashed_bytes),
        };
        Ok(encoded)
    }
}
