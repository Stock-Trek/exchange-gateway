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
    real: String,
    paper: String,
}

#[derive(Debug, Display, Clone, Copy, Serialize, Deserialize)]
pub enum ExchangeTransportType {
    Http,
    Websocket,
}

#[derive(Debug, Display, Clone, Copy, Serialize, Deserialize)]
pub enum TradingMode {
    Real,
    Paper,
}

impl ExchangeTransportUrls {
    pub fn new(real: &str, paper: &str) -> Self {
        Self {
            real: real.into(),
            paper: paper.into(),
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
    pub fn url(&self, transport_type: ExchangeTransportType, trading_mode: TradingMode) -> String {
        let env_var_name = format!(
            "{}_{}_{}",
            self.name.to_uppercase(),
            transport_type.to_string().to_uppercase(),
            trading_mode.to_string().to_uppercase()
        );
        std::env::var(env_var_name)
            .unwrap_or_else(|_| self.default_url(transport_type, trading_mode).into())
    }
    fn default_url(
        &self,
        transport_type: ExchangeTransportType,
        trading_mode: TradingMode,
    ) -> &str {
        let transport_urls = match transport_type {
            ExchangeTransportType::Http => &self.http,
            ExchangeTransportType::Websocket => &self.websocket,
        };
        match trading_mode {
            TradingMode::Real => &transport_urls.real,
            TradingMode::Paper => &transport_urls.paper,
        }
    }
}
