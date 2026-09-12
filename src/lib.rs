#[cfg(feature = "auto-resync")]
pub mod auto_resync_connector;
pub mod clients;
mod clock;
pub mod connector;
pub mod error;
pub mod functions;
mod panic_guard;
pub mod rate_limit;
mod retry_after;
pub mod submission;
mod websocket_listener;

pub use async_trait::async_trait;
#[cfg(feature = "iris")]
pub use iris;

pub mod prelude {
    #[cfg(feature = "auto-resync")]
    pub use crate::auto_resync_connector::AutoResyncConnector;
    pub use crate::{
        connector::Connector,
        error::{EGError, EGResult, SendFailure},
        submission::{Submission, SubmissionId, SubmissionOutcome},
    };
    pub use exchange_types::urls::TradingMode;
}
