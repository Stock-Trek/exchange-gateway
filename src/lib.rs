pub mod authenticate_leg;
pub mod connect;
pub mod connector;
pub mod connector_impl;
pub mod credentials;
pub mod error;
pub mod functions;
pub mod listeners;
pub mod rate_limit;
pub mod sign;
pub mod specs;
pub mod transports;
pub mod urls;

pub mod prelude {
    pub use crate::{
        connect::Connect,
        connector_impl::ConnectorImpl,
        credentials::{api_key_credential::ApiKeyCredentials, jwt_credential::JwtCredentials},
        error::{EGError, EGResult},
        functions::ArcTryConvertValue,
        listeners::listener::ListenerTrait,
        transports::{http::HttpClientTrait, websocket::WebsocketClientTrait},
        urls::TradingMode,
    };
}
