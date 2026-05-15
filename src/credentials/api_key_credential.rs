use crate::{credentials::credential::Credential, destroy::Destroy};

pub struct ApiKeyCredentials {
    pub api_key: String,
    secret: Vec<u8>,
}

impl ApiKeyCredentials {
    pub fn new(api_key: String, secret: Vec<u8>) -> Self {
        Self { api_key, secret }
    }
}

impl Credential for ApiKeyCredentials {
    fn credential(&self) -> &Vec<u8> {
        &self.secret
    }
}

impl Destroy for ApiKeyCredentials {
    fn destroy(&mut self) {
        self.secret.clear();
    }
}
