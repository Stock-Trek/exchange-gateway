use secrecy::SecretString;

use crate::credentials::Credential;

pub struct ApiKeyCredentials {
    pub api_key: SecretString,
    pub secret: SecretString,
}

impl ApiKeyCredentials {
    pub fn new(api_key: SecretString, secret: SecretString) -> Self {
        Self { api_key, secret }
    }
}

impl Credential for ApiKeyCredentials {}
