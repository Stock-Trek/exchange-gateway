use crate::{
    error::{EGError, EGResult},
    sign::encrypt::{
        data_signer::DataSigner, ecdsa_p256::EcdsaP256Signer, ecdsa_p384::EcdsaP384Signer,
        ed25519::Ed25519Signer, hmac_sha256::HmacSha256Signer, hmac_sha512::HmacSha512Signer,
    },
};
use secrecy::{ExposeSecret, SecretSlice, SecretString};
use std::sync::Arc;
use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SigningAlgorithm {
    EcdsaP256,
    EcdsaP384,
    Ed25519,
    HmacSha256,
    HmacSha512,
}

impl SigningAlgorithm {
    pub fn signer(&self, key: &SecretString) -> EGResult<DataSigner> {
        match self {
            Self::EcdsaP256 => {
                let key_bytes = key.expose_secret().as_bytes();
                let signing_key = p256::ecdsa::SigningKey::from_slice(key_bytes)
                    .map_err(|e| EGError::Custom(format!("ECDSA P-256 key error: {e}")))?;
                Ok(Arc::new(EcdsaP256Signer::new(signing_key)))
            }
            Self::EcdsaP384 => {
                let key_bytes = key.expose_secret().as_bytes();
                let signing_key = p384::ecdsa::SigningKey::from_slice(key_bytes)
                    .map_err(|e| EGError::Custom(format!("ECDSA P-384 key error: {e}")))?;
                Ok(Arc::new(EcdsaP384Signer::new(signing_key)))
            }
            Self::Ed25519 => {
                let key_bytes = key.expose_secret().as_bytes();
                let signing_key =
                    ed25519_compact::SecretKey::from_slice(key_bytes).map_err(|_| {
                        EGError::Custom("Ed25519 key must be exactly 32 bytes".to_string())
                    })?;
                Ok(Arc::new(Ed25519Signer::new(signing_key)))
            }
            Self::HmacSha256 => {
                let key_vec = key.expose_secret().as_bytes().to_vec();
                let hmac_slice = SecretSlice::from(key_vec);
                Ok(Arc::new(HmacSha256Signer::new(hmac_slice)))
            }
            Self::HmacSha512 => {
                let key_vec = key.expose_secret().as_bytes().to_vec();
                let hmac_slice = SecretSlice::from(key_vec);
                Ok(Arc::new(HmacSha512Signer::new(hmac_slice)))
            }
        }
    }
}
