use exchange_types::urls::{Protocol, TradingMode, Urls};

pub(crate) struct BoxedUrls(pub(crate) Box<dyn Urls>);

impl Urls for BoxedUrls {
    fn name(&self) -> &'static str {
        self.0.name()
    }
    fn default(&self, protocol: Protocol, trading_mode: TradingMode) -> &str {
        self.0.default(protocol, trading_mode)
    }
}
