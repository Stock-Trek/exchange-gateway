use secrecy::SecretString;

use crate::credentials::Credential;

pub struct UsernamePasswordCredentials {
    pub username: String,
    pub password: SecretString,
}

impl UsernamePasswordCredentials {
    pub fn new(username: String, password: SecretString) -> Self {
        Self { username, password }
    }
}

impl Credential for UsernamePasswordCredentials {}
