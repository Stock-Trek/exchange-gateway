use secrecy::SecretString;
#[cfg(feature = "serde")]
use serde::Deserialize;

#[cfg_attr(feature = "serde", derive(Deserialize))]
pub struct JwtCredentials {
    pub api_key: SecretString,
    pub secret: SecretString,
}
