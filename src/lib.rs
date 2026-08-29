// The transports are optional features. When not all transports are enabled
// the dormant transport machinery would otherwise be reported as dead code,
// so only lint it once every transport is compiled in.
#![cfg_attr(not(all(feature = "reqwest", feature = "iris")), allow(dead_code))]

pub mod auth_gate;
pub mod authenticate_leg;
pub mod clock;
pub mod connect;
pub mod connector;
pub mod connector_impl;
pub mod credentials;
pub mod error;
pub mod functions;
pub mod listeners;
pub mod rate_limit;
pub mod resync;
pub mod sign;
mod specs;
mod transports;
pub mod urls;

pub mod prelude {
    pub use crate::{
        clock::Clock,
        connect::Connect,
        connector::Connector,
        connector_impl::ConnectorImpl,
        credentials::{api_key_credential::ApiKeyCredentials, jwt_credential::JwtCredentials},
        error::{EGError, EGResult},
        functions::ArcTryConvertValue,
        listeners::listener::ListenerTrait,
        urls::TradingMode,
    };
}
