use crate::{
    connector::Connector,
    error::EGResult,
    functions::BoxTryCreateOnce,
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
    specs::binance::{
        http::connector as binance_http_connector,
        websocket::connector as binance_websocket_connector,
    },
    transports::{
        http::{HttpClientTrait, HttpRequest, HttpResponse},
        websocket::WebsocketClientTrait,
    },
};
use exchange_types::{
    binance::{
        http::{BinanceHttpRequest, BinanceHttpResponse},
        websocket::{BinanceWebsocketRequest, BinanceWebsocketResponse},
    },
    urls::TradingMode,
};
use std::sync::Arc;

#[cfg(feature = "iris")]
use {crate::transports::iris::IrisWebsocketClient, iris::Config as IrisConfig};

#[cfg(feature = "reqwest")]
use crate::transports::reqwest::ReqwestHttpClient;

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[cfg(feature = "reqwest")]
    pub fn binance_http_reqwest(
        trading_mode: TradingMode,
    ) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
        binance_http_connector(
            trading_mode,
            Box::new(|url| Ok(ReqwestHttpClient::new(&url))),
        )
    }

    pub fn binance_http(
        trading_mode: TradingMode,
        client_creator: BoxTryCreateOnce<
            String,
            impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
        >,
    ) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
        binance_http_connector(trading_mode, client_creator)
    }

    #[cfg(feature = "iris")]
    pub fn binance_websocket_iris(
        trading_mode: TradingMode,
        listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
        iris_config: IrisConfig,
    ) -> EGResult<impl Connector<BinanceWebsocketRequest, BinanceWebsocketResponse>> {
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
        binance_websocket_connector(trading_mode, listener, client_creator)
    }

    pub fn binance_websocket(
        trading_mode: TradingMode,
        listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
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
    ) -> EGResult<impl Connector<BinanceWebsocketRequest, BinanceWebsocketResponse>> {
        binance_websocket_connector(trading_mode, listener, client_creator)
    }
}
