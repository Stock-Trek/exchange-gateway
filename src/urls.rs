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

pub(crate) struct LocalhostUrls;

impl Urls for LocalhostUrls {
    fn name(&self) -> &'static str {
        "localhost"
    }
    fn url(&self, protocol: Protocol, _trading_mode: TradingMode) -> &str {
        match protocol {
            Protocol::Http => "http://localhost",
            Protocol::Websocket => "ws://localhost",
        }
    }
}

pub(crate) struct BoxedUrls(pub(crate) Box<dyn Urls>);

impl Urls for BoxedUrls {
    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn url(&self, protocol: Protocol, trading_mode: TradingMode) -> &str {
        self.0.url(protocol, trading_mode)
    }
}
