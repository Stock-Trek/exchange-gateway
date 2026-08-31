#[cfg(feature = "iris")]
use {
    crate::{
        listeners::listener::ListenerTrait,
        specs::binance::websocket::{
            connector as binance_websocket_connector,
            connector_with_client_factory as binance_websocket_connector_with_client_factory,
        },
        transports::websocket::WebsocketClientTrait,
    },
    exchange_types::binance::websocket::{
        BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
    iris::Config as IrisConfig,
    std::time::Duration,
};

#[cfg(feature = "reqwest")]
use {
    crate::{
        specs::binance::http::{
            connector as binance_http_connector,
            connector_with_client as binance_http_connector_with_client,
        },
        transports::{
            http::HttpClientTrait,
            reqwest::{HttpRequest, HttpResponse},
        },
    },
    exchange_types::binance::http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
};

#[cfg(any(feature = "iris", feature = "reqwest"))]
use crate::functions::TryConvertValue;
#[cfg(any(feature = "iris", feature = "reqwest"))]
use crate::{
    clock::Clock, connector::Connector, credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult, urls::TradingMode,
};
#[cfg(any(feature = "iris", feature = "reqwest"))]
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[cfg(feature = "reqwest")]
    pub fn binance_http<ExternalReq, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + 'static,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        binance_http_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            credentials,
            clock,
        )
    }

    /// Builds a Binance HTTP connector backed by a caller-provided client,
    /// so production users can configure proxies/TLS/timeouts (via
    /// [`crate::transports::reqwest::ReqwestHttpClient::with_client`]) or
    /// inject their own [`crate::transports::http::HttpClientTrait`]
    /// implementation. The caller is responsible for resolving the base URL,
    /// e.g. `exchange_urls().url(ExchangeTransportType::Http, trading_mode)`.
    #[cfg(feature = "reqwest")]
    pub fn binance_http_with_client<ExternalReq, ExternalRes>(
        &self,
        client: Arc<dyn HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse>>,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + 'static,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        binance_http_connector_with_client(
            client,
            to_unsigned_request,
            to_external_response,
            credentials,
            clock,
        )
    }

    #[cfg(feature = "iris")]
    pub fn binance_websocket<ExternalReq, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: impl ListenerTrait<TMessage = ExternalRes> + 'static,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
        use_session: bool,
        iris_config: IrisConfig,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + Sync,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        binance_websocket_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
            clock,
            use_session,
            iris_config,
        )
    }

    /// Builds a Binance WebSocket connector backed by a caller-provided
    /// client factory, so production users can inject their own
    /// [`crate::transports::websocket::WebsocketClientTrait`] implementation
    /// (e.g. a custom websocket stack) instead of the default
    /// [`crate::transports::iris::IrisWebsocketClient`]. The factory receives
    /// the exchange-response listener the client must deliver responses to.
    #[cfg(feature = "iris")]
    pub fn binance_websocket_with_client_factory<ExternalReq, ExternalRes>(
        &self,
        client_factory: impl FnOnce(
            Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
        ) -> Arc<
            dyn WebsocketClientTrait<
                    TransportReq = BinanceWebsocketRequest,
                    TransportRes = BinanceWebsocketResponse,
                >,
        >,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: impl ListenerTrait<TMessage = ExternalRes> + 'static,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
        use_session: bool,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + Sync,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        binance_websocket_connector_with_client_factory(
            client_factory,
            Duration::from_secs(20),
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
            clock,
            use_session,
        )
    }
}
