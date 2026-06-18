use chrono::Utc;
use p256::ecdsa::SigningKey;
use p256::ecdsa::signature::Signer;
use secrecy::ExposeSecret;
use serde::Serialize;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

use crate::credentials::jwt_credential::JwtCredentials;

/// Builds a Coinbase Cloud API JWT bearer token from the given credentials.
///
/// Uses ECDSA P-256 (ES256) signing as required by Coinbase's JWT authentication.
/// The JWT is constructed with:
/// - `sub` and `kid` set to the API key name
/// - `iss` set to "coinbase-cloud"
/// - `aud` set to `["rest.coinbase.com"]`
/// - `iat` set to current time
/// - `exp` set to current time + 120 seconds
/// - Signed using raw R||S format (64 bytes) per ES256 spec
pub fn build_coinbase_jwt(credentials: &JwtCredentials) -> StockTrekResult<String> {
    let api_key = &credentials.api_key;
    let signing_key = SigningKey::from_slice(credentials.secret.expose_secret().as_bytes())
        .map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "Failed to create Coinbase ECDSA P-256 signing key: {e}"
            )))
        })?;

    let now = Utc::now().timestamp();
    let payload = CoinbaseJwtPayload {
        sub: api_key.clone(),
        iss: "coinbase-cloud".to_string(),
        aud: vec!["rest.coinbase.com".to_string()],
        iat: now,
        exp: now + 120,
    };

    // Build JWT header: {"alg":"ES256","kid":"<api_key>","typ":"JWT"}
    let header = serde_json::json!({
        "alg": "ES256",
        "kid": api_key,
        "typ": "JWT",
    });

    // Base64url-encode header and payload
    let header_b64 = base64url_encode(&serde_json::to_vec(&header).map_err(|e| {
        StockTrekError::General(GeneralError::Message(format!(
            "Failed to serialize JWT header: {e}"
        )))
    })?);
    let payload_b64 = base64url_encode(&serde_json::to_vec(&payload).map_err(|e| {
        StockTrekError::General(GeneralError::Message(format!(
            "Failed to serialize JWT payload: {e}"
        )))
    })?);

    // Sign the "header.payload" string using ECDSA P-256 (ES256)
    let signing_input = format!("{header_b64}.{payload_b64}");
    let signature: p256::ecdsa::Signature = signing_key.sign(signing_input.as_bytes());
    // ES256 uses raw R||S format (64 bytes)
    let signature_b64 = base64url_encode(&signature.to_vec());

    Ok(format!("{signing_input}.{signature_b64}"))
}

/// The unsigned JWT payload for Coinbase Cloud API authentication.
/// https://docs.cdp.coinbase.com/advanced-trade/docs/rest-api-auth
#[derive(Serialize)]
struct CoinbaseJwtPayload {
    sub: String,
    iss: String,
    #[serde(rename = "aud")]
    aud: Vec<String>,
    iat: i64,
    exp: i64,
}

/// Base64url-encode bytes (no padding, URL-safe).
fn base64url_encode(data: &[u8]) -> String {
    data_encoding::BASE64URL_NOPAD.encode(data)
}
