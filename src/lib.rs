pub mod authenticator;
pub mod authenticator_creator;
pub mod connector;
pub mod connectors;
pub mod converter;
pub mod credentials;
pub mod error;
pub mod functions;
pub mod listeners;
pub mod rate_limit;
pub mod sign;
pub mod specs;
pub mod transports;

pub mod prelude {
    pub use crate::{
        authenticator::Authenticator,
        authenticator_creator::AuthenticatorCreator,
        connector::Connector,
        connectors::Connectors,
        error::{EGError, EGResult},
        functions::{TryConvertRequestTo, TryConvertResponseFrom},
        transports::{
            http_transport::{
                HttpClient, HttpClientTrait, HttpMessageDto, HttpTransport, HttpTransportCreator,
            },
            transport::TransportTrait,
            transport_creator::{TransportCreator, TransportCreatorTrait},
            websocket_transport::{
                WebsocketClient, WebsocketClientTrait, WebsocketMessageDto, WebsocketTransport,
                WebsocketTransportCreator,
            },
        },
    };
}
