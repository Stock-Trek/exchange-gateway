use secrecy::SecretString;

/// Credentials for JWT-based API authentication (e.g. Coinbase Cloud API).
///
/// - `api_key` is the API key name (used as the `kid` in the JWT header)
/// - `secret` contains the PEM-encoded ECDSA P-256 private key bytes
pub struct JwtCredentials {
    pub api_key: String,
    pub secret: SecretString,
}

impl JwtCredentials {
    pub fn new(api_key: String, secret: SecretString) -> Self {
        Self { api_key, secret }
    }
}
