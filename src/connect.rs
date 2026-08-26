#[cfg(any(feature = "reqwest", feature = "iris"))]
use crate::{
    connector::Connector, credentials::api_key_credential::ApiKeyCredentials, error::EGResult,
    functions::ArcTryConvertValue, listeners::listener::ListenerTrait, urls::TradingMode,
};
#[cfg(any(feature = "reqwest", feature = "iris"))]
use std::sync::Arc;

#[cfg(feature = "reqwest")]
use crate::specs::binance::http_connector;
#[cfg(feature = "iris")]
use crate::specs::binance::websocket_connector;
#[cfg(feature = "reqwest")]
use exchange_types::binance::http::{BinanceHttpResponse, BinanceHttpUnsignedRequest};
#[cfg(feature = "iris")]
use exchange_types::binance::websocket::{
    BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
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
        websocket_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
            use_session,
        )
    }
}
