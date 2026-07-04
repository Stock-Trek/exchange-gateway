pub mod authenticator;
pub mod authenticator_creator;
pub mod authenticators;
pub mod connector;
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
        authenticators::Authenticators,
        connector::Connector,
        error::{EGError, EGResult},
        functions::{TryConvertRequestTo, TryConvertResponseFrom},
        transports::{
            http_transport::{HttpClient, HttpClientTrait, HttpMessageDto, HttpTransportCreator},
            transport::TransportTrait,
            transport_creator::{TransportCreator, TransportCreatorTrait},
            websocket_transport::{
                WebsocketClient, WebsocketClientTrait, WebsocketMessageDto,
                WebsocketTransportCreator,
            },
        },
    };
}
