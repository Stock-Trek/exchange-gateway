mod auto_resync;
pub mod clients;
pub mod clock;
pub mod connector;
pub mod connector_builder;
pub mod error;
pub mod functions;
pub mod listeners;
mod panic_guard;
pub mod rate_limit;
mod urls;

pub use async_trait::async_trait;
#[cfg(feature = "iris")]
pub use iris;

pub mod prelude {
    pub use crate::{
        clock::Clock,
        connector::Connector,
        connector_builder::ConnectorBuilder,
        error::{EGError, EGResult},
        listeners::listener::ListenerTrait,
    };
    pub use exchange_types::urls::TradingMode;
}
