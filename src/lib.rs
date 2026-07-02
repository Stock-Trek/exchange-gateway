pub mod authenticator;
pub mod authenticator_creator;
pub mod connector;
pub mod connectors;
pub mod converter;
pub mod credentials;
pub mod error;
pub mod functions;
pub mod listeners;
pub mod messenger;
pub mod rate_limit;
pub mod sign;
pub mod specs;
pub mod time_ordered_id;
pub mod transports;

pub mod prelude {
    pub use crate::{
        authenticator::Authenticator,
        connector::Connector,
        // connectors::Connectors, TODO
        error::{EGError, EGResult},
        functions::{TryConvertRequestTo, TryConvertResponseFrom},
    };
}
