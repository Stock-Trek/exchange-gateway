use sha2::{Sha256, Sha512};
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};
use strum::Display;

/// Supported signing algorithms for message signing.
///
/// Each variant represents a distinct cryptographic signing scheme.
/// The enum is extensible — new algorithms can be added as new variants.
#[derive(Debug, Display, Clone, PartialEq, Eq, Hash)]
pub enum SigningAlgorithm {
    /// HMAC with SHA-256 hash
    HmacSha256,
    /// HMAC with SHA-512 hash
    HmacSha512,
    /// ECDSA using the P-256 (secp256r1) curve
    EcdsaP256,
    /// ECDSA using the P-384 (secp384r1) curve
    EcdsaP384,
    /// Ed25519 signature scheme
    Ed25519,
}

/// A trait for pluggable signing algorithm implementations.
///
/// Implementations must be stateless — the algorithm itself carries no key material.
/// Key material is provided at call time via `key`.
pub trait Signer: Send + Sync {
    /// Sign the given `message` bytes using `key` as the signing key/secret.
    ///
    /// Returns the raw signature bytes.
    fn sign(&self, message: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>>;

    /// Return the `SigningAlgorithm` this signer implements.
    fn algorithm(&self) -> SigningAlgorithm;
}

/// HMAC-SHA256 signer implementation.
pub struct HmacSha256Signer;

impl Signer for HmacSha256Signer {
    fn sign(&self, message: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "HMAC-SHA256 key error: {e}"
            )))
        })?;
        mac.update(message);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    fn algorithm(&self) -> SigningAlgorithm {
        SigningAlgorithm::HmacSha256
    }
}

/// HMAC-SHA512 signer implementation.
pub struct HmacSha512Signer;

impl Signer for HmacSha512Signer {
    fn sign(&self, message: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        use hmac::{Hmac, Mac};
        type HmacSha512 = Hmac<Sha512>;
        let mut mac = HmacSha512::new_from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "HMAC-SHA512 key error: {e}"
            )))
        })?;
        mac.update(message);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    fn algorithm(&self) -> SigningAlgorithm {
        SigningAlgorithm::HmacSha512
    }
}

/// ECDSA P-256 signer implementation.
pub struct EcdsaP256Signer;

impl Signer for EcdsaP256Signer {
    fn sign(&self, message: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        use p256::ecdsa::{SigningKey, signature::Signer as _};
        let signing_key = SigningKey::from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "ECDSA P-256 key error: {e}"
            )))
        })?;
        let signature: p256::ecdsa::Signature = signing_key.sign(message);
        Ok(signature.to_der().to_bytes().to_vec())
    }

    fn algorithm(&self) -> SigningAlgorithm {
        SigningAlgorithm::EcdsaP256
    }
}

/// ECDSA P-384 signer implementation.
pub struct EcdsaP384Signer;

impl Signer for EcdsaP384Signer {
    fn sign(&self, message: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        use p384::ecdsa::{SigningKey, signature::Signer as _};
        let signing_key = SigningKey::from_slice(key).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "ECDSA P-384 key error: {e}"
            )))
        })?;
        let signature: p384::ecdsa::Signature = signing_key.sign(message);
        Ok(signature.to_der().to_bytes().to_vec())
    }

    fn algorithm(&self) -> SigningAlgorithm {
        SigningAlgorithm::EcdsaP384
    }
}

/// Ed25519 signer implementation.
///
/// The key material is expected to be the 32-byte secret key (seed).
pub struct Ed25519Signer;

impl Signer for Ed25519Signer {
    fn sign(&self, message: &[u8], key: &[u8]) -> StockTrekResult<Vec<u8>> {
        let key_bytes: [u8; 32] = key.try_into().map_err(|_| {
            StockTrekError::General(GeneralError::Message(
                "Ed25519 key must be exactly 32 bytes".to_string(),
            ))
        })?;
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_bytes);
        use ed25519_dalek::Signer as _;
        let signature = signing_key.sign(message);
        Ok(signature.to_bytes().to_vec())
    }

    fn algorithm(&self) -> SigningAlgorithm {
        SigningAlgorithm::Ed25519
    }
}

/// Factory to create a `Box<dyn Signer>` from a `SigningAlgorithm`.
pub fn signer_from_algorithm(algorithm: SigningAlgorithm) -> Box<dyn Signer> {
    match algorithm {
        SigningAlgorithm::HmacSha256 => Box::new(HmacSha256Signer),
        SigningAlgorithm::HmacSha512 => Box::new(HmacSha512Signer),
        SigningAlgorithm::EcdsaP256 => Box::new(EcdsaP256Signer),
        SigningAlgorithm::EcdsaP384 => Box::new(EcdsaP384Signer),
        SigningAlgorithm::Ed25519 => Box::new(Ed25519Signer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_sha256_sign() {
        let signer = HmacSha256Signer;
        let key = b"my-secret-key";
        let msg = b"hello world";
        let sig = signer.sign(msg, key).unwrap();
        assert!(!sig.is_empty());

        // Verify using the same HMAC
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.verify_slice(&sig).unwrap();
    }

    #[test]
    fn test_hmac_sha512_sign() {
        let signer = HmacSha512Signer;
        let key = b"my-secret-key";
        let msg = b"hello world";
        let sig = signer.sign(msg, key).unwrap();
        assert!(!sig.is_empty());
    }

    #[test]
    fn test_ed25519_sign() {
        // Use a deterministic 32-byte secret key for testing
        let key_bytes: [u8; 32] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
            0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
            0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
        ];

        let signer = Ed25519Signer;
        let msg = b"hello world";
        let sig = signer.sign(msg, &key_bytes).unwrap();
        assert_eq!(sig.len(), 64);

        // Verify the signature
        use ed25519_dalek::{SigningKey, VerifyingKey};
        let signing_key = SigningKey::from_bytes(&key_bytes);
        let verifying_key = VerifyingKey::from(&signing_key);
        let sig_array: [u8; 64] = sig.as_slice().try_into().unwrap();
        let ed_sig = ed25519_dalek::Signature::from_bytes(&sig_array);
        use ed25519_dalek::ed25519::signature::Verifier as _;
        verifying_key.verify(msg, &ed_sig).unwrap();
    }
}
