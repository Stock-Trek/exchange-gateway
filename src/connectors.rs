use crate::{
    connector::Connector,
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::ArcTryConvertValue,
    listeners::listener::Listener,
    specs::binance::{BinanceHttpConnectorCreator, BinanceWebsocketConnectorCreator},
    transports::{http::CreateHttpClient, websocket::CreateWebsocketClient},
    urls::TradingMode,
};
use exchange_types::binance::{
    http::{BinanceHttpBody, BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    websocket::{
        BinanceWebsocketBody, BinanceWebsocketRequest, BinanceWebsocketResponse,
        BinanceWebsocketUnsignedRequest,
    },
};

#[derive(Debug, Clone)]
pub struct Connectors;

impl Connectors {
    pub fn binance_http<TRequest, TResponse>(
        &self,
        trading_mode: TradingMode,
        client_creator: CreateHttpClient<BinanceHttpBody>,
        convert_request: ArcTryConvertValue<TRequest, BinanceHttpUnsignedRequest>,
        convert_response: ArcTryConvertValue<BinanceHttpResponse, TResponse>,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
            BinanceHttpBody,
            BinanceHttpResponse,
            TResponse,
        >,
    >
    where
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceHttpConnectorCreator {
            client_creator,
            to_response: convert_response,
            to_unsigned: convert_request,
        }
        .into_connector(trading_mode, listener)
    }
    pub fn binance_websocket<TRequest, TResponse>(
        &self,
        trading_mode: TradingMode,
        client_creator: CreateWebsocketClient<BinanceWebsocketBody>,
        convert_request: ArcTryConvertValue<TRequest, BinanceWebsocketUnsignedRequest>,
        convert_response: ArcTryConvertValue<BinanceWebsocketResponse, TResponse>,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            BinanceWebsocketRequest,
            BinanceWebsocketBody,
            BinanceWebsocketResponse,
            TResponse,
        >,
    >
    where
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceWebsocketConnectorCreator {
            client_creator,
            to_response: convert_response,
            to_unsigned: convert_request,
        }
        .into_connector(trading_mode, listener)
    }
}
