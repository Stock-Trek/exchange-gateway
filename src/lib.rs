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
        credentials::{api_key_credential::ApiKeyCredentials, jwt_credential::JwtCredentials},
        error::{EGError, EGResult},
        functions::ArcTryConvertValue,
        listeners::listener::{Listener, ListenerTrait},
        transports::{
            http::{CreateHttpClient, HttpClientMarker, HttpClientTrait, HttpMessageDto},
            websocket::{
                CreateWebsocketClient, WebsocketClientMarker, WebsocketClientTrait,
                WebsocketMessageDto,
            },
        },
    };
}
