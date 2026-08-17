use crate::{
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    functions::ArcTryConvertValue,
    listeners::listener::ListenerTrait,
    specs::binance::{HttpConnectorBuilder, WebsocketConnectorBuilder},
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
        create_client: impl Fn(&str) -> TClient + 'static,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_transport_request: ArcTryConvertValue<BinanceHttpRequest, HttpReq>,
        to_binance_response: ArcTryConvertValue<HttpRes, BinanceHttpResponse>,
        to_external_response: ArcTryConvertValue<BinanceHttpResponse, ExternalRes>,
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
        HttpConnectorBuilder::new(create_client)
            .trading_mode(trading_mode)
            .to_unsigned_request(to_unsigned_request)
            .to_transport_request(to_transport_request)
            .to_binance_response(to_binance_response)
            .to_external_response(to_external_response)
            .listener(listener)
            .credentials(credentials)
            .build()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn binance_websocket<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        create_client: impl Fn(&str, Arc<dyn ListenerTrait<TMessage = WebsocketRes>>) -> TClient
        + 'static,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_transport_request: ArcTryConvertValue<BinanceWebsocketRequest, WebsocketReq>,
        to_binance_response: ArcTryConvertValue<WebsocketRes, BinanceWebsocketResponse>,
        to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
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
        WebsocketConnectorBuilder::new(create_client)
            .trading_mode(trading_mode)
            .to_unsigned_request(to_unsigned_request)
            .to_transport_request(to_transport_request)
            .to_binance_response(to_binance_response)
            .to_external_response(to_external_response)
            .listener(listener)
            .credentials(credentials)
            .use_session(use_session)
            .build()
    }
}
