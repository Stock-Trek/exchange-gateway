use secrecy::SecretString;
#[cfg(feature = "serde")]
use serde::Deserialize;

#[cfg_attr(feature = "serde", derive(Deserialize))]
pub struct ApiKeyCredentials {
    pub api_key: String,
    pub secret: SecretString,
}
