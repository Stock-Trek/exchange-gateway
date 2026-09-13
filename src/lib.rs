#[cfg(feature = "auto-resync")]
pub mod auto_resync_connector;
pub mod clients;
mod clock;
pub mod connector;
pub mod error;
mod panic_guard;
pub mod rate_limit;
mod retry_after;
pub mod submission;
pub mod websocket_listener;

pub use websocket_listener::{WaiterForResponse, WebsocketListener};

pub use async_trait::async_trait;
#[cfg(feature = "iris")]
pub use iris;

pub mod prelude {
    #[cfg(feature = "auto-resync")]
    pub use crate::auto_resync_connector::AutoResyncConnector;
    pub use crate::{
        clients::client::WebsocketClient,
        connector::Connector,
        error::{EGError, EGResult, SendFailure},
        submission::{Submission, SubmissionOutcome},
        websocket_listener::{WaiterForResponse, WebsocketListener},
    };
    pub use exchange_types::urls::TradingMode;
}
