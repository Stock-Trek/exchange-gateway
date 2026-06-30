use secrecy::SecretString;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct ApiKeyCredentials {
    pub api_key: String,
    pub secret: SecretString,
}
