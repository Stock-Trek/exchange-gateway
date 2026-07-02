// use crate::{
//     authenticator::Authenticator,
//     authenticator_creator::AuthenticatorCreator,
//     credentials::api_key_credential::ApiKeyCredentials,
//     functions::TryConvertToResponse,
//     specs::binance::{BinanceHttpAuthenticatorCreator, BinanceWebsocketAuthenticatorCreator},
// };
// use chrono::Duration;
// use exchange_types::binance::{
//     http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
//     websocket::{BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest},
// };
// use std::marker::PhantomData;

// pub struct Connectors;

// impl Connectors {
//     pub fn binance_http<TTransport, TRequest, TResponse>(
//         &self,
//         transport: TTransport,
//         request_timeout: Duration,
//         to_response: TryConvertToResponse<BinanceHttpResponse, TResponse>,
//     ) -> Authenticator<TRequest, BinanceHttpUnsignedRequest, ApiKeyCredentials, TResponse>
//     where
//         TTransport: HttpTransportTrait + 'static,
//         TRequest: Send + Sync + 'static,
//         TResponse: Send + Sync + 'static,
//     {
//         BinanceHttpAuthenticatorCreator {
//             transport,
//             request_timeout,
//             to_response,
//             _phantom_request: PhantomData,
//         }
//         .into_authenticator()
//     }
//     pub fn binance_websocket<TTransport, TRequest, TResponse>(
//         &self,
//         transport: TTransport,
//         request_timeout: Duration,
//         to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
//         use_session: bool,
//     ) -> Authenticator<TRequest, BinanceWebsocketUnsignedRequest, ApiKeyCredentials, TResponse>
//     where
//         TTransport: WebsocketTransportTrait + 'static,
//         TRequest: Send + Sync + 'static,
//         TResponse: Send + Sync + 'static,
//     {
//         BinanceWebsocketAuthenticatorCreator {
//             transport,
//             request_timeout,
//             to_response,
//             use_session,
//             _phantom_request: PhantomData,
//         }
//         .into_authenticator()
//     }
// }
