#[cfg(feature = "iris")]
use {
    crate::{clock::Clock, specs::binance::websocket::connector as binance_websocket_connector},
    exchange_types::binance::websocket::{
        BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
    iris::Config as IrisConfig,
};

#[cfg(feature = "reqwest")]
use {
    crate::specs::binance::http::connector as binance_http_connector,
    exchange_types::binance::http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
};

#[cfg(feature = "reqwest")]
use crate::functions::TryConvertValue;
#[cfg(any(feature = "iris", feature = "reqwest"))]
use crate::{
    connector::Connector, credentials::api_key_credential::ApiKeyCredentials, error::EGResult,
    listeners::listener::ListenerTrait, urls::TradingMode,
};

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[cfg(feature = "reqwest")]
    pub fn binance_http<ExternalReq, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
        listener: impl ListenerTrait<TMessage = ExternalRes> + 'static,
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
            listener,
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
}
