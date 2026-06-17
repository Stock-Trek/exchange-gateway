use secrecy::SecretString;

pub struct ApiKeyCredentials {
    pub api_key: String,
    pub secret: SecretString,
}

impl ApiKeyCredentials {
    pub fn new(api_key: String, secret: SecretString) -> Self {
        Self { api_key, secret }
    }
}
