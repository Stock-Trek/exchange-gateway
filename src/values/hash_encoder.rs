use base64::{Engine, engine::general_purpose};
use sha2::{Digest, Sha256, Sha512};
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct HasherEncoder<THashableValue, TToBytes>
where
    TToBytes: Fn(&THashableValue) -> &Vec<u8>,
{
    to_bytes: TToBytes,
    hash_algorithm: HashAlgorithm,
    encoding: Encoding,
    _phantom_hashable_value: PhantomData<THashableValue>,
}

pub enum HashAlgorithm {
    Sha256,
    Sha512,
}

pub enum Encoding {
    Hex,
    Base64,
}

impl<THashableValue, TToBytes> HasherEncoder<THashableValue, TToBytes>
where
    TToBytes: Fn(&THashableValue) -> &Vec<u8>,
{
    pub fn new(to_bytes: TToBytes, hash_algorithm: HashAlgorithm, encoding: Encoding) -> Self {
        Self {
            to_bytes,
            hash_algorithm,
            encoding,
            _phantom_hashable_value: PhantomData,
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
