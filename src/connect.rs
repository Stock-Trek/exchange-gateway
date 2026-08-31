use crate::{
    clock::Clock,
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{BoxTryCreateOnce, TryConvertValue},
    transports::websocket::WebsocketClientTrait,
    urls::TradingMode,
};
use std::sync::Arc;

#[cfg(feature = "iris")]
use {
    crate::{
        listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
        specs::binance::websocket::connector as binance_websocket_connector,
        transports::iris::IrisWebsocketClient,
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
        specs::binance::http::connector as binance_http_connector,
        transports::{
            http::HttpClientTrait,
            reqwest::{HttpRequest, HttpResponse, ReqwestHttpClient},
        },
    },
    exchange_types::binance::http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
};

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[cfg(feature = "reqwest")]
    pub fn binance_http_reqwest<ExternalReq, ExternalRes>(
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
            Box::new(|url| Ok(ReqwestHttpClient::new(&url))),
        )
    }
    pub fn binance_http<ExternalReq, ExternalRes>(
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
        client_creator: BoxTryCreateOnce<
            String,
            impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
        >,
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
            client_creator,
        )
    }

    #[cfg(feature = "iris")]
    pub fn binance_websocket_iris<ExternalReq, ExternalRes>(
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: impl ListenerTrait<TMessage = ExternalRes> + 'static,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
        use_session: bool,
        logon_timeout: Duration,
        iris_config: IrisConfig,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + Sync,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        let client_creator = Box::new(
            move |(url, websocket_listener): (
                String,
                Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
            )| {
                let client =
                IrisWebsocketClient::<BinanceWebsocketRequest, BinanceWebsocketResponse>::with_config(
                    &url,
                    iris_config,
                    websocket_listener,
                );
                Ok(client)
            },
        );
        binance_websocket_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            credentials,
            clock,
            listener,
            use_session,
            logon_timeout,
            client_creator,
        )
    }
    pub fn binance_websocket<ExternalReq, ExternalRes>(
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
        listener: impl ListenerTrait<TMessage = ExternalRes> + 'static,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
        use_session: bool,
        logon_timeout: Duration,
        client_creator: BoxTryCreateOnce<
            (
                String,
                Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
            ),
            impl WebsocketClientTrait<
                TransportReq = BinanceWebsocketRequest,
                TransportRes = BinanceWebsocketResponse,
            > + 'static,
        >,
    ) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
    where
        ExternalReq: Send + Sync,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        binance_websocket_connector(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            credentials,
            clock,
            listener,
            use_session,
            logon_timeout,
            client_creator,
        )
    }
}
