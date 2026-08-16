use crate::{
    connector::Connector, credentials::api_key_credential::ApiKeyCredentials,
    functions::TryConvertValue, listeners::listener::ListenerTrait,
    specs::binance::websocket_connector, transports::websocket::WebsocketClientTrait,
    urls::TradingMode,
};
use exchange_types::binance::websocket::{
    BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    pub fn binance_websocket<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes>(
        &self,
        trading_mode: TradingMode,
        create_client: impl Fn(
            &str,
            Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
        ) -> TClient,
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
