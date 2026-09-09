mod auto_resync;
mod boxed_urls;
pub mod clients;
mod clock;
pub mod connector;
pub mod connector_builder;
pub mod error;
pub mod functions;
mod panic_guard;
pub mod rate_limit;
mod websocket_listener;

pub use async_trait::async_trait;
#[cfg(feature = "iris")]
pub use iris;

pub mod prelude {
    pub use crate::{
        clock::Clock,
        connector::Connector,
        connector_builder::ConnectorBuilder,
        error::{EGError, EGResult},
    };
    pub use exchange_types::urls::TradingMode;
}
