use crate::{
    boxed_urls::BoxedUrls,
    clients::{
        client::{HttpClient, WebsocketClient},
        iris::IrisWebsocketClient,
        reqwest::ReqwestHttpClient,
    },
    connector::Connector,
    error::EGResult,
    functions::BoxTryCreateOnce,
    rate_limit::boxed::BoxedRateLimits,
    websocket_listener::WebsocketListener,
};
use exchange_types::{
    encode::ByteEncoder,
    rate_limited::{RateLimits, UnlimitedRateLimits},
    signer::Signer,
    urls::{LocalhostUrls, TradingMode, Urls},
};
use iris::Config as IrisConfig;
use std::sync::Arc;

pub struct ConnectorBuilder {
    trading_mode: TradingMode,
    urls: Box<dyn Urls>,
    rate_limits: Box<dyn RateLimits>,
    signer: Signer,
}

impl ConnectorBuilder {
    pub fn new() -> Self {
        Self {
            trading_mode: TradingMode::Paper,
            urls: Box::new(LocalhostUrls),
            rate_limits: Box::new(UnlimitedRateLimits),
            signer: Signer::new_unencrypted(ByteEncoder::Base64),
        }
    }
    #[cfg(feature = "iris")]
    #[allow(clippy::type_complexity)]
    pub fn build_websocket_iris(
        self,
        iris_config: IrisConfig,
    ) -> EGResult<Connector<(IrisWebsocketClient, Arc<WebsocketListener>)>> {
        let client_creator: BoxTryCreateOnce<
            (String, Arc<WebsocketListener>),
            IrisWebsocketClient,
        > = Box::new(move |(url, websocket_listener)| {
            Ok(IrisWebsocketClient::with_config(
                &url,
                iris_config,
                websocket_listener,
            ))
        });
        self.build_websocket(client_creator)
    }
}

impl Default for ConnectorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorBuilder {
    pub fn trading_mode(mut self, trading_mode: TradingMode) -> Self {
        self.trading_mode = trading_mode;
        self
    }
    pub fn urls(mut self, urls: impl Urls + 'static) -> Self {
        self.urls = Box::new(urls);
        self
    }
    pub fn rate_limits(mut self, rate_limits: impl RateLimits + 'static) -> Self {
        self.rate_limits = Box::new(rate_limits);
        self
    }
    pub fn signer(mut self, signer: Signer) -> Self {
        self.signer = signer;
        self
    }
    #[cfg(feature = "reqwest")]
    pub fn build_http_reqwest(self) -> EGResult<Connector<ReqwestHttpClient>> {
        let client_creator = Box::new(move |url: String| Ok(ReqwestHttpClient::new(&url)));
        self.build_http(client_creator)
    }
    pub fn build_http<C>(
        self,
        client_creator: BoxTryCreateOnce<String, C>,
    ) -> EGResult<Connector<C>>
    where
        C: HttpClient,
    {
        Connector::try_new_http(
            self.trading_mode,
            &BoxedUrls(self.urls),
            BoxedRateLimits(self.rate_limits),
            self.signer,
            client_creator,
        )
    }
    #[allow(clippy::type_complexity)]
    pub fn build_websocket<C>(
        self,
        client_creator: BoxTryCreateOnce<(String, Arc<WebsocketListener>), C>,
    ) -> EGResult<Connector<(C, Arc<WebsocketListener>)>>
    where
        C: WebsocketClient,
    {
        Connector::try_new_websocket(
            self.trading_mode,
            &BoxedUrls(self.urls),
            BoxedRateLimits(self.rate_limits),
            self.signer,
            client_creator,
        )
    }
}
