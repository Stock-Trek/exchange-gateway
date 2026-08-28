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
