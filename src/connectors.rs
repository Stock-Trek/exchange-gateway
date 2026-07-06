use crate::{
    connector::Connector,
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{TryConvertRequestTo, TryConvertResponseFrom},
    listeners::listener::Listener,
    specs::binance::{BinanceHttpConnectorCreator, BinanceWebsocketConnectorCreator},
    transports::{
        http_transport::HttpMessageDto, transport::TransportTrait,
        transport_creator::TransportCreator, websocket_transport::WebsocketMessageDto,
    },
};
use exchange_types::binance::{
    http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    websocket::{
        BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
};
use std::time::Duration;

pub struct Connectors;

impl Connectors {
    pub fn binance_http<TTransport, TRequest, TResponse>(
        &self,
        transport_creator: TransportCreator<TTransport, HttpMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceHttpUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceHttpResponse, TResponse>,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
            TTransport,
            BinanceHttpResponse,
            TResponse,
        >,
    >
    where
        TTransport: TransportTrait<MessageDto = HttpMessageDto> + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceHttpConnectorCreator {
            request_timeout,
            transport_creator,
            to_response: convert_response,
            to_unsigned: convert_request,
        }
        .into_connector(listener)
    }
    pub fn binance_websocket<TTransport, TRequest, TResponse>(
        &self,
        transport_creator: TransportCreator<TTransport, WebsocketMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            BinanceWebsocketRequest,
            TTransport,
            BinanceWebsocketResponse,
            TResponse,
        >,
    >
    where
        TTransport: TransportTrait<MessageDto = WebsocketMessageDto> + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceWebsocketConnectorCreator {
            request_timeout,
            transport_creator,
            to_response: convert_response,
            to_unsigned: convert_request,
        }
        .into_connector(listener)
    }
}
