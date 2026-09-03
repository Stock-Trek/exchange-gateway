use exchange_types::urls::{Protocol, TradingMode, Urls};

pub(crate) fn url(urls: &impl Urls, protocol: Protocol, trading_mode: TradingMode) -> String {
    let env_var_name = format!(
        "{}_{}_{}",
        urls.name().to_uppercase(),
        protocol.to_string().to_uppercase(),
        trading_mode.to_string().to_uppercase()
    );
    std::env::var(env_var_name).unwrap_or_else(|_| urls.url(protocol, trading_mode).into())
}
