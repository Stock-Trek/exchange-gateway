use crate::{
    connector::ConnectorCreator,
    credentials::api_key_credential::ApiKeyCredentials,
    functions::{TryConvertFromRequest, TryConvertToResponse},
    specs::binance_websocket::BinanceWebsocketSpecCreator,
    transports::websocket_transport::WebsocketTransportTrait,
};
use exchange_types::binance::{
    signed::BinanceUnsignedParams,
    websocket::{
        BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
};
use std::marker::PhantomData;

pub struct Connectors;

impl Connectors {
    pub async fn binance_websocket<TTransport, TRequest, TResponse>(
        &self,
        credentials: ApiKeyCredentials,
        transport: TTransport,
        use_session: bool,
        to_binance_params: TryConvertFromRequest<TRequest, BinanceUnsignedParams>,
        to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
    ) -> ConnectorCreator<
        BinanceWebsocketSpecCreator<TTransport, TRequest, TResponse>,
        TRequest,
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
        TResponse,
    >
    where
        TTransport: WebsocketTransportTrait + 'static,
        TRequest: Send + Sync + 'static,
        TResponse: Send + Sync + 'static,
    {
        let spec_creator = BinanceWebsocketSpecCreator {
            credentials,
            transport,
            use_session,
            to_binance_params,
            to_response,
        };
        ConnectorCreator {
            spec_creator,
            _phantom_request: PhantomData,
            _phantom_signed_message: PhantomData,
            _phantom_unsigned_message: PhantomData,
            _phantom_response: PhantomData,
        }
    }
}
