// The transports are optional features. When not all transports are enabled
// the dormant transport machinery would otherwise be reported as dead code,
// so only lint it once every transport is compiled in.
#![cfg_attr(not(all(feature = "reqwest", feature = "iris")), allow(dead_code))]

pub mod clock;
pub mod connect;
pub mod connector;
pub mod connector_impl;
pub mod error;
pub mod functions;
pub mod listeners;
pub mod rate_limiter;
mod specs;
mod transports;
pub mod urls;

pub mod prelude {
    pub use crate::{
        clock::Clock,
        connect::Connect,
        connector::Connector,
        connector_impl::ConnectorImpl,
        error::{EGError, EGResult},
        functions::ArcTryConvertValue,
        listeners::listener::ListenerTrait,
    };
}
