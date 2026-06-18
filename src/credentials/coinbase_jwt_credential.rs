use p256::ecdsa::SigningKey;
use secrecy::{ExposeSecret, SecretString};

/// Credentials for Coinbase Cloud API JWT authentication.
///
/// Uses an ECDSA P-256 key pair where:
/// - `api_key` is the Coinbase Cloud API key name (used as the `kid` in the JWT header)
/// - `secret` contains the PEM-encoded ECDSA P-256 private key bytes
pub struct CoinbaseJwtCredentials {
    pub api_key: String,
    pub signing_key: SigningKey,
    pub _secret: SecretString,
}

impl CoinbaseJwtCredentials {
    pub fn new(api_key: String, secret: SecretString) -> Self {
        let signing_key = SigningKey::from_slice(secret.expose_secret().as_bytes())
            .expect("Failed to create Coinbase ECDSA P-256 signing key");
        Self {
            api_key,
            signing_key,
            _secret: secret,
        }
    }
}
