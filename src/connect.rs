use {
    crate::{
        clock::Clock, connector::Connector, error::EGResult, functions::BoxTryCreateOnce,
        rate_limiter::RateLimiter, transports::websocket::WebsocketClientTrait,
    },
    exchange_types::urls::TradingMode,
    std::sync::Arc,
};

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
        specs::binance::http::connector as binance_http_connector,
        transports::{
            http::HttpClientTrait,
            reqwest::{HttpRequest, HttpResponse, ReqwestHttpClient},
        },
    },
    exchange_types::binance::http::{BinanceHttpRequest, BinanceHttpResponse},
};

#[derive(Debug, Clone)]
pub struct Connect;

impl Connect {
    #[cfg(feature = "reqwest")]
    pub fn binance_http_reqwest(
        trading_mode: TradingMode,
        clock: Clock,
        rate_limiter: Arc<dyn RateLimiter>,
    ) -> EGResult<impl Connector<Request = BinanceHttpRequest, Response = BinanceHttpResponse>>
    {
        binance_http_connector(
            trading_mode,
            clock,
            rate_limiter,
            Box::new(|url| Ok(ReqwestHttpClient::new(&url))),
        )
    }
    pub fn binance_http(
        trading_mode: TradingMode,
        clock: Clock,
        rate_limiter: Arc<dyn RateLimiter>,
        client_creator: BoxTryCreateOnce<
            &str,
            impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
        >,
    ) -> EGResult<impl Connector<Request = BinanceHttpRequest, Response = BinanceHttpResponse>>
    {
        binance_http_connector(trading_mode, clock, rate_limiter, client_creator)
    }

    #[cfg(feature = "iris")]
    pub fn binance_websocket_iris(
        trading_mode: TradingMode,
        clock: Clock,
        rate_limiter: Arc<dyn RateLimiter>,
        listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
        iris_config: IrisConfig,
    ) -> EGResult<
        impl Connector<Request = BinanceWebsocketRequest, Response = BinanceWebsocketResponse>,
    > {
        let client_creator = Box::new(
            move |(url, websocket_listener): (
                &str,
                Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
            )| {
                let client =
                IrisWebsocketClient::<BinanceWebsocketRequest, BinanceWebsocketResponse>::with_config(
                    url,
                    iris_config,
                    websocket_listener,
                );
                Ok(client)
            },
        );
        binance_websocket_connector(trading_mode, clock, rate_limiter, listener, client_creator)
    }
    pub fn binance_websocket(
        trading_mode: TradingMode,
        clock: Clock,
        rate_limiter: Arc<dyn RateLimiter>,
        listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
        client_creator: BoxTryCreateOnce<
            (
                &str,
                Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
            ),
            impl WebsocketClientTrait<
                TransportReq = BinanceWebsocketRequest,
                TransportRes = BinanceWebsocketResponse,
            > + 'static,
        >,
    ) -> EGResult<
        impl Connector<Request = BinanceWebsocketRequest, Response = BinanceWebsocketResponse>,
    > {
        binance_websocket_connector(trading_mode, clock, rate_limiter, listener, client_creator)
    }
}
