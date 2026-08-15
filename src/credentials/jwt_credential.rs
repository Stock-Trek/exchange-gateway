use secrecy::SecretString;
#[cfg(feature = "serde")]
use serde::Deserialize;

/// Credentials for JWT-based API authentication (e.g. Coinbase Cloud API).
///
/// - `api_key` is the API key name (used as the `kid` in the JWT header)
/// - `secret` contains the PEM-encoded ECDSA P-256 private key bytes
#[cfg_attr(feature = "serde", derive(Deserialize))]
pub struct JwtCredentials {
    pub api_key: SecretString,
    pub secret: SecretString,
}
