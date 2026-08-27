#[cfg(feature = "iris")]
use {
    crate::{specs::binance::websocket_connector, transports::iris::default_config},
    exchange_types::binance::websocket::{
        BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
    iris::Config as IrisConfig,
};

#[cfg(feature = "reqwest")]
use {
    crate::specs::binance::http_connector,
    exchange_types::binance::http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
};

#[cfg(any(feature = "iris", feature = "reqwest"))]
use {
    crate::{
        connector::Connector, credentials::api_key_credential::ApiKeyCredentials, error::EGResult,
        functions::ArcTryConvertValue, listeners::listener::ListenerTrait, urls::TradingMode,
    },
    std::sync::Arc,
};

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    /// Builds a connector backed by the built-in [`reqwest`] HTTP transport.
    ///
    /// The transport is private to the crate: the gateway signs the request and
    /// builds the transport-level HTTP request internally, so the signed query
    /// string and the sent request can never diverge.
    #[cfg(feature = "reqwest")]
    pub fn binance_http<ExternalReq, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_external_response: ArcTryConvertValue<BinanceHttpResponse, ExternalRes>,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
        credentials: Option<ApiKeyCredentials>,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        http_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
        )
    }

    /// Builds a connector backed by the built-in [`iris`] websocket transport.
    ///
    /// The transport is private to the crate: the gateway signs the request and
    /// serializes the exchange-level request internally, so the signed payload
    /// and the sent message can never diverge.
    ///
    /// The iris client is configured to reconnect automatically after a
    /// graceful server close (e.g. Binance maintenance or session expiry), so
    /// the connector's reconnect/re-authentication machinery keeps working.
    /// Pass a custom [`IrisConfig`] via
    /// [`binance_websocket_with_config`](Connect::binance_websocket_with_config)
    /// to tune the reconnect/circuit-breaker behavior.
    #[cfg(feature = "iris")]
    pub fn binance_websocket<ExternalReq, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
        credentials: Option<ApiKeyCredentials>,
        use_session: bool,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + Sync,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        self.binance_websocket_with_config(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
            use_session,
            default_config(),
        )
    }

    /// Builds a connector backed by the built-in [`iris`] websocket transport
    /// using a custom [`IrisConfig`].
    ///
    /// The transport is private to the crate: the gateway signs the request and
    /// serializes the exchange-level request internally, so the signed payload
    /// and the sent message can never diverge.
    ///
    /// The connector's reconnect/re-authentication machinery depends on
    /// `on_connected` firing again after a drop, so the config should use
    /// [`iris::ServerCloseBehavior::Reconnect`] (the default chosen by
    /// [`binance_websocket`](Connect::binance_websocket)). A config that keeps
    /// iris's default `Disconnect` behavior permanently ends the connection
    /// task on a clean server close, so reconnects and re-authentication never
    /// engage.
    #[cfg(feature = "iris")]
    #[allow(clippy::too_many_arguments)]
    pub fn binance_websocket_with_config<ExternalReq, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
        credentials: Option<ApiKeyCredentials>,
        use_session: bool,
        iris_config: IrisConfig,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + Sync,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        websocket_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
            use_session,
            iris_config,
        )
    }
}
