use serde::{Deserialize, Serialize};
use strum::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExchangeUrls {
    name: String,
    http: ExchangeTransportUrls,
    websocket: ExchangeTransportUrls,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ExchangeTransportUrls {
    live: String,
    test: String,
}

#[derive(Debug, Display, Clone, Copy, Serialize, Deserialize)]
pub enum ExchangeTransportType {
    HTTP,
    WEBSOCKET,
}

#[derive(Debug, Display, Clone, Copy, Serialize, Deserialize)]
pub enum ExchangeNetType {
    LIVE,
    TEST,
}

impl ExchangeTransportUrls {
    pub fn new(live: &str, test: &str) -> Self {
        Self {
            live: live.into(),
            test: test.into(),
        }
    }
}

impl ExchangeUrls {
    pub fn new(name: &str, http: ExchangeTransportUrls, websocket: ExchangeTransportUrls) -> Self {
        Self {
            name: name.into(),
            http,
            websocket,
        }
    }
    pub fn url(&self, transport_type: ExchangeTransportType, net_type: ExchangeNetType) -> String {
        let env_var_name = format!("{}_{}_{}", self.name, transport_type, net_type);
        std::env::var(env_var_name)
            .unwrap_or_else(|_| self.default_url(transport_type, net_type).into())
    }
    fn default_url(
        &self,
        transport_type: ExchangeTransportType,
        net_type: ExchangeNetType,
    ) -> &str {
        let transport_urls = match transport_type {
            ExchangeTransportType::HTTP => &self.http,
            ExchangeTransportType::WEBSOCKET => &self.websocket,
        };
        match net_type {
            ExchangeNetType::LIVE => &transport_urls.live,
            ExchangeNetType::TEST => &transport_urls.test,
        }
    }
}
