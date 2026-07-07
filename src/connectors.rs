use crate::{
    connector::Connector,
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{CreateClient, TryConvertRequestTo, TryConvertResponseFrom},
    listeners::listener::Listener,
    specs::binance::{BinanceHttpConnectorCreator, BinanceWebsocketConnectorCreator},
    transports::{
        http_client::{HttpClient, HttpMessageDto},
        websocket_client::{WebsocketClient, WebsocketMessageDto},
    },
};
use exchange_types::binance::{
    http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    websocket::{
        BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
};

pub struct Connectors;

impl Connectors {
    pub fn binance_http<TTransport, TRequest, TResponse>(
        &self,
        client_creator: CreateClient<HttpClient, HttpMessageDto>,
        convert_request: TryConvertRequestTo<TRequest, BinanceHttpUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceHttpResponse, TResponse>,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
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
        .into_connector(listener)
    }
    pub fn binance_websocket<TRequest, TResponse>(
        &self,
        client_creator: CreateClient<WebsocketClient, WebsocketMessageDto>,
        convert_request: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            BinanceWebsocketRequest,
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
        .into_connector(listener)
    }
}
