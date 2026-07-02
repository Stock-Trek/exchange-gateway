use crate::{
    authenticator_creator::AuthenticatorCreator,
    converter::Converter,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{TryConvertRequestTo, TryConvertResponseFrom},
    specs::binance::{BinanceHttpAuthenticatorCreator, BinanceWebsocketAuthenticatorCreator},
    transports::{
        http_transport::HttpMessageDto, transport::TransportTrait,
        transport_creator::TransportCreator, websocket_transport::WebsocketMessageDto,
    },
};
use chrono::Duration;
use exchange_types::binance::{
    http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
    websocket::{BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest},
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
        AuthenticatorCreator<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            HttpMessageDto,
            BinanceHttpResponse,
            TResponse,
        >,
    >
    where
        TTransport: TransportTrait<MessageDto = HttpMessageDto> + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        let converter = Converter {
            convert_request,
            convert_response,
        };
        let creator = BinanceHttpAuthenticatorCreator {
            request_timeout,
            transport_creator,
            converter,
        };
        Ok(Box::new(creator))
    }
    pub fn binance_websocket<TTransport, TRequest, TResponse>(
        &self,
        transport_creator: TransportCreator<TTransport, WebsocketMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
        use_session: bool,
    ) -> EGResult<
        AuthenticatorCreator<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            WebsocketMessageDto,
            BinanceWebsocketResponse,
            TResponse,
        >,
    >
    where
        TTransport: TransportTrait<MessageDto = WebsocketMessageDto> + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        let converter = Converter {
            convert_request,
            convert_response,
        };
        let creator = BinanceWebsocketAuthenticatorCreator {
            request_timeout,
            transport_creator,
            converter,
            use_session,
        };
        Ok(Box::new(creator))
    }
}
