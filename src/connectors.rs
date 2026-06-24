use crate::{
    authenticator::Authenticator, authenticator_creator::AuthenticatorCreator,
    credentials::api_key_credential::ApiKeyCredentials, functions::TryConvertToResponse,
    specs::binance_websocket::BinanceWebsocketAuthenticatorCreator,
    transports::websocket_transport::WebsocketTransportTrait,
};
use chrono::Duration;
use exchange_types::binance::websocket::{
    BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
};
use std::marker::PhantomData;

pub struct Connectors;

impl Connectors {
    pub fn binance_websocket<TTransport, TRequest, TResponse>(
        &self,
        use_session: bool,
        connector_timeout: Duration,
        transport: TTransport,
        to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
    ) -> Authenticator<TRequest, BinanceWebsocketUnsignedRequest, ApiKeyCredentials, TResponse>
    where
        TTransport: WebsocketTransportTrait + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        BinanceWebsocketAuthenticatorCreator {
            connector_timeout,
            to_response,
            transport,
            use_session,
            _phantom_request: PhantomData,
        }
        .into_authenticator()
    }
}
