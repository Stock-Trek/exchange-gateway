pub mod connector;
pub mod connector_creator;
pub mod connector_session;
pub mod connectors;
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
        connector::Connector,
        connector_session::ConnectorSession,
        connectors::Connectors,
        error::{EGError, EGResult},
        functions::{TryConvertRequestTo, TryConvertResponseFrom},
        transports::{
            http_client::{HttpClient, HttpClientTrait, HttpMessageDto},
            transport::TransportMessageDto,
            websocket_client::{WebsocketClient, WebsocketClientTrait, WebsocketMessageDto},
        },
    };
}
