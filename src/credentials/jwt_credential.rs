use p256::ecdsa::SigningKey;
use secrecy::{ExposeSecret, SecretString};

/// Credentials for JWT-based API authentication (e.g. Coinbase Cloud API).
///
/// Uses an ECDSA P-256 key pair where:
/// - `api_key` is the API key name (used as the `kid` in the JWT header)
/// - `secret` contains the PEM-encoded ECDSA P-256 private key bytes
pub struct JwtCredentials {
    pub api_key: String,
    pub signing_key: SigningKey,
    pub _secret: SecretString,
}

impl JwtCredentials {
    pub fn new(api_key: String, secret: SecretString) -> Self {
        let signing_key = SigningKey::from_slice(secret.expose_secret().as_bytes())
            .expect("Failed to create ECDSA P-256 signing key from secret");
        Self {
            api_key,
            signing_key,
            _secret: secret,
        }
    }
}
