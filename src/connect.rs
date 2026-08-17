use crate::{
    connector::Connector, credentials::api_key_credential::ApiKeyCredentials,
    functions::TryConvertValue, listeners::listener::ListenerTrait,
    specs::binance::{http_connector, websocket_connector},
    transports::{http::HttpClientTrait, websocket::WebsocketClientTrait},
    urls::TradingMode,
};
use exchange_types::binance::http::{
    BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest,
};
use exchange_types::binance::websocket::{
    BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[allow(clippy::too_many_arguments)]
    pub fn binance_http<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        create_client: impl Fn(&str) -> TClient,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_transport_request: TryConvertValue<BinanceHttpRequest, HttpReq>,
        to_binance_response: TryConvertValue<HttpRes, BinanceHttpResponse>,
        to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
        credentials: Option<ApiKeyCredentials>,
    ) -> impl Connector<ExternalReq, ExternalRes>
    where
        TClient: HttpClientTrait<TransportReq = HttpReq, TransportRes = HttpRes> + 'static,
        ExternalReq: Send,
        HttpReq: Send,
        HttpRes: Send + 'static,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        http_connector(
            trading_mode,
            create_client,
            to_unsigned_request,
            to_transport_request,
            to_binance_response,
            to_external_response,
            listener,
            credentials,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn binance_websocket<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        create_client: impl Fn(&str, Arc<dyn ListenerTrait<TMessage = WebsocketRes>>) -> TClient,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_transport_request: TryConvertValue<BinanceWebsocketRequest, WebsocketReq>,
        to_binance_response: TryConvertValue<WebsocketRes, BinanceWebsocketResponse>,
        to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
        credentials: Option<ApiKeyCredentials>,
        use_session: bool,
    ) -> impl Connector<ExternalReq, ExternalRes>
    where
        TClient: WebsocketClientTrait<TransportReq = WebsocketReq, TransportRes = WebsocketRes>
            + 'static,
        ExternalReq: Send + Sync,
        WebsocketReq: Send,
        WebsocketRes: Send + 'static,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        websocket_connector(
            trading_mode,
            create_client,
            to_unsigned_request,
            to_transport_request,
            to_binance_response,
            to_external_response,
            listener,
            credentials,
            use_session,
        )
    }
}
