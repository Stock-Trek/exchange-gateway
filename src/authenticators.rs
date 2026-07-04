use crate::{
    authenticator::Authenticator,
    authenticator_creator::AuthenticatorCreatorTrait,
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

pub struct Authenticators;

impl Authenticators {
    pub fn binance_http<TTransport, TRequest, TResponse>(
        &self,
        transport_creator: TransportCreator<TTransport, HttpMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceHttpUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceHttpResponse, TResponse>,
    ) -> EGResult<Authenticator<TRequest, ApiKeyCredentials, TResponse>>
    where
        TTransport: TransportTrait<MessageDto = HttpMessageDto> + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceHttpAuthenticatorCreator {
            request_timeout,
            transport_creator,
            to_response: convert_response,
            to_unsigned: convert_request,
        }
        .into_authenticator()
    }
    pub fn binance_websocket<TTransport, TRequest, TResponse>(
        &self,
        transport_creator: TransportCreator<TTransport, WebsocketMessageDto>,
        request_timeout: Duration,
        convert_request: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
        convert_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
        use_session: bool,
    ) -> EGResult<Authenticator<TRequest, ApiKeyCredentials, TResponse>>
    where
        TTransport: TransportTrait<MessageDto = WebsocketMessageDto> + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceWebsocketAuthenticatorCreator {
            request_timeout,
            transport_creator,
            to_response: convert_response,
            to_unsigned: convert_request,
            use_session,
        }
        .into_authenticator()
    }
}
