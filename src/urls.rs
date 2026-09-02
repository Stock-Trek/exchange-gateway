use exchange_types::urls::{Protocol, TradingMode, Urls};

#[derive(Debug, Clone, Copy)]
pub struct ExchangeUrls {}

impl ExchangeUrls {
    pub fn url(&self, urls: &impl Urls, protocol: Protocol, trading_mode: TradingMode) -> String {
        let exchange_name = urls.name().to_uppercase();
        let env_var_name = format!(
            "{}_{}_{}",
            exchange_name,
            protocol.to_string().to_uppercase(),
            trading_mode.to_string().to_uppercase()
        );
        std::env::var(env_var_name).unwrap_or_else(|_| urls.url(protocol, trading_mode).into())
    }
}
