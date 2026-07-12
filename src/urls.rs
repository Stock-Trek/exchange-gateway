use strum::Display;

#[derive(Debug, Clone)]
pub(crate) struct ExchangeUrls {
    name: String,
    live: String,
    test: String,
}

#[derive(Debug, Display, Clone, Copy)]
pub enum ExchangeNetType {
    LIVE,
    TEST,
}

impl ExchangeUrls {
    pub fn new(name: &str, live: &str, test: &str) -> Self {
        Self {
            name: name.into(),
            live: live.into(),
            test: test.into(),
        }
    }
    pub fn url(&self, net_type: ExchangeNetType) -> String {
        let env_var_name = format!("{}_{}", self.name, net_type);
        std::env::var(env_var_name).unwrap_or_else(|_| self.default_url(net_type).into())
    }
    fn default_url(&self, net_type: ExchangeNetType) -> &str {
        match net_type {
            ExchangeNetType::LIVE => &self.live,
            ExchangeNetType::TEST => &self.test,
        }
    }
}
