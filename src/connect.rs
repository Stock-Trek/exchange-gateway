use crate::{clock::Clock, connector::Connector, error::EGResult, urls::TradingMode};
use std::sync::Arc;

#[cfg(feature = "iris")]
use {
    crate::{
        listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
        specs::binance::websocket::connector as binance_websocket_connector,
        transports::iris::IrisWebsocketClient,
    },
    exchange_types::binance::websocket::{BinanceWebsocketRequest, BinanceWebsocketResponse},
    iris::Config as IrisConfig,
};

#[cfg(feature = "reqwest")]
use {
    crate::{
        functions::BoxTryCreateOnce,
        specs::binance::http::connector as binance_http_connector,
        transports::{
            http::HttpClientTrait,
            reqwest::{HttpRequest, HttpResponse, ReqwestHttpClient},
        },
    },
    exchange_types::binance::http::{BinanceHttpRequest, BinanceHttpResponse},
};

#[cfg(feature = "iris")]
use crate::transports::websocket::WebsocketClientTrait;

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[cfg(feature = "reqwest")]
    pub fn binance_http_reqwest(
        trading_mode: TradingMode,
        clock: Clock,
    ) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
        binance_http_connector(
            trading_mode,
            clock,
            Box::new(|url| Ok(ReqwestHttpClient::new(&url))),
        )
    }
    #[cfg(feature = "reqwest")]
    pub fn binance_http(
        trading_mode: TradingMode,
        clock: Clock,
        client_creator: BoxTryCreateOnce<
            String,
            impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
        >,
    ) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
        binance_http_connector(trading_mode, clock, client_creator)
    }

    #[cfg(feature = "iris")]
    pub fn binance_websocket_iris(
        trading_mode: TradingMode,
        clock: Clock,
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
        binance_websocket_connector(trading_mode, clock, listener, client_creator)
    }
    #[cfg(feature = "iris")]
    pub fn binance_websocket(
        trading_mode: TradingMode,
        clock: Clock,
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
        binance_websocket_connector(trading_mode, clock, listener, client_creator)
    }
}
