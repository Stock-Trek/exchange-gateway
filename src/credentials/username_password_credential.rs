use crate::{credentials::credential::Credential, destroy::Destroy};

pub struct UsernamePasswordCredentials {
    pub username: String,
    password: Vec<u8>,
}

impl UsernamePasswordCredentials {
    pub fn new(username: String, password: Vec<u8>) -> Self {
        Self { username, password }
    }
}

impl Credential for UsernamePasswordCredentials {
    fn credential(&self) -> &Vec<u8> {
        &self.password
    }
}

impl Destroy for UsernamePasswordCredentials {
    fn destroy(mut self) {
        self.password.clear();
    }
}
