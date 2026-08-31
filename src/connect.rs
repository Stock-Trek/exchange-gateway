use std::sync::Arc;

use crate::{
    clock::Clock,
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{ArcTryConvertValue, TryConvertValue},
    urls::TradingMode,
};

#[cfg(feature = "iris")]
use {
    crate::{
        listeners::listener::ListenerTrait,
        specs::binance::{
            common::exchange_urls, websocket::connector as binance_websocket_connector,
        },
        transports::{iris::IrisWebsocketClient, websocket::WebsocketClientTrait},
        urls::ExchangeTransportType,
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
            Arc::new(|url| Ok(ReqwestHttpClient::new(&url))),
        )
    }
    pub fn binance_http<ExternalReq, ExternalRes>(
        trading_mode: TradingMode,
        to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
        to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
        credentials: Option<ApiKeyCredentials>,
        clock: Clock,
        client_creator: ArcTryConvertValue<
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
        &self,
        trading_mode: TradingMode,
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
        self.binance_websocket(
            trading_mode,
            to_unsigned_request,
            to_external_response,
            listener,
            credentials,
            clock,
            use_session,
            IrisConfig::default(),
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
        let url = exchange_urls().url(ExchangeTransportType::Websocket, trading_mode);
        let client_factory = move |websocket_listener| -> Arc<
            dyn WebsocketClientTrait<
                    TransportReq = BinanceWebsocketRequest,
                    TransportRes = BinanceWebsocketResponse,
                >,
        > {
            let client =
                IrisWebsocketClient::<BinanceWebsocketRequest, BinanceWebsocketResponse>::with_config(
                    &url,
                    iris_config,
                    websocket_listener,
                );
            Arc::new(client)
        };
        binance_websocket_connector(
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
