use crate::{
    clients::{
        client::{HttpClient, WebsocketClient},
        iris::IrisWebsocketClient,
        reqwest::ReqwestHttpClient,
    },
    connector::Connector,
    error::EGResult,
    functions::{ArcTryConvertValue, BoxTryCreateOnce},
    listeners::{
        boxed::BoxedListener, listener::ListenerTrait, no_op::NoOpListener,
        websocket_listener::WebsocketListener,
    },
    rate_limit::{boxed::BoxedRateLimits, unlimited::NoRateLimits},
    urls::{BoxedUrls, LocalhostUrls},
};
use exchange_types::{
    rate_limited::RateLimits,
    signer::Signer,
    urls::{TradingMode, Urls},
};
use iris::Config as IrisConfig;
use std::sync::Arc;

pub struct ConnectorBuilder<TransportRes = serde_json::Value> {
    trading_mode: TradingMode,
    urls: Box<dyn Urls>,
    rate_limits: Box<dyn RateLimits>,
    signer: Signer,
    converter: ArcTryConvertValue<TransportRes, serde_json::Value>,
    listener: Box<dyn ListenerTrait<TMessage = serde_json::Value>>,
}

impl ConnectorBuilder<serde_json::Value> {
    pub fn new() -> Self {
        Self {
            trading_mode: TradingMode::Paper,
            urls: Box::new(LocalhostUrls),
            rate_limits: Box::new(NoRateLimits),
            signer: NoOpListener::noop_signer(),
            converter: Arc::new(|value: serde_json::Value| -> EGResult<serde_json::Value> {
                Ok(value)
            }),
            listener: Box::new(NoOpListener),
        }
    }
    #[cfg(feature = "iris")]
    #[allow(clippy::type_complexity)]
    pub fn build_websocket_iris(
        self,
        iris_config: IrisConfig,
    ) -> EGResult<
        Connector<(
            IrisWebsocketClient,
            Arc<WebsocketListener<serde_json::Value, serde_json::Value>>,
        )>,
    > {
        let client_creator: BoxTryCreateOnce<
            (
                String,
                Arc<WebsocketListener<serde_json::Value, serde_json::Value>>,
            ),
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

impl Default for ConnectorBuilder<serde_json::Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TransportRes> ConnectorBuilder<TransportRes> {
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
    pub fn listener(
        mut self,
        listener: impl ListenerTrait<TMessage = serde_json::Value> + 'static,
    ) -> Self {
        self.listener = Box::new(listener);
        self
    }
    pub fn converter<NewTransportRes>(
        self,
        converter: ArcTryConvertValue<NewTransportRes, serde_json::Value>,
    ) -> ConnectorBuilder<NewTransportRes> {
        ConnectorBuilder {
            trading_mode: self.trading_mode,
            urls: self.urls,
            rate_limits: self.rate_limits,
            signer: self.signer,
            listener: self.listener,
            converter,
        }
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
        client_creator: BoxTryCreateOnce<
            (
                String,
                Arc<WebsocketListener<TransportRes, serde_json::Value>>,
            ),
            C,
        >,
    ) -> EGResult<Connector<(C, Arc<WebsocketListener<TransportRes, serde_json::Value>>)>>
    where
        C: WebsocketClient,
    {
        Connector::try_new_websocket(
            self.trading_mode,
            &BoxedUrls(self.urls),
            BoxedRateLimits(self.rate_limits),
            self.signer,
            self.converter,
            BoxedListener(self.listener),
            client_creator,
        )
    }
}
