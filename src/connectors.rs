use crate::{
    connector::Connector,
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{TryConvertRequestTo, TryConvertResponseFrom},
    specs::binance::{BinanceHttpConnectorCreator, BinanceWebsocketConnectorCreator},
    transports::{
        http_transport::HttpMessageDto, transport::TransportTrait,
        transport_creator::TransportCreator, websocket_transport::WebsocketMessageDto,
    },
};
use chrono::Duration;
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
        transport_creator: TransportCreator<TTransport, HttpMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceHttpUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceHttpResponse, TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
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
        .into_connector()
    }
    pub fn binance_websocket<TTransport, TRequest, TResponse>(
        &self,
        transport_creator: TransportCreator<TTransport, WebsocketMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            BinanceWebsocketRequest,
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
        .into_connector()
    }
}
