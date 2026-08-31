use crate::rate_limit::feedback::RateLimitFeedback;
use std::time::SystemTimeError;

/// The default external error type carried by [`EGError::External`]: a
/// boxed, type-erased error. Concrete error types (e.g. `reqwest::Error`,
/// `iris::ConnectionError`) are preserved by parameterising [`EGError`] with
/// their type instead.
pub type ExternalError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type EGResult<T, E = ExternalError> = Result<T, EGError<E>>;

/// The gateway error type. Parameterised over the external error carried by
/// the [`EGError::External`] variant so transport-level errors keep their
/// concrete type; every other variant is independent of `E`. The default
/// `E = ExternalError` preserves the previous boxed behaviour for callers
/// that do not care about the concrete type.
#[derive(Debug, thiserror::Error)]
pub enum EGError<E = ExternalError> {
    #[error("Exchange API error {code}: {message}")]
    ApiError { code: i64, message: String },
    #[error("Received unrecognised response")]
    BadResponse,
    #[error("Clock error: {0}")]
    ClockError(SystemTimeError),
    #[error("Crypto key error: {0}")]
    CryptoKey(String),
    #[error(transparent)]
    External(#[from] E),
    #[error(
        "HTTP request failed with status {status}: {body}",
        body = String::from_utf8_lossy(body)
    )]
    HttpError { status: u16, body: Vec<u8> },
    #[error("Internal mutex poisoned by a panicking operation")]
    MutexPoisoned,
    #[error("Connector is not authenticated")]
    NotAuthenticated,
    #[error("Connector is not connected")]
    NotConnected,
    #[error("Rate limit exceeded")]
    RateLimited(RateLimitFeedback),
    #[error("Request timed out waiting for a response")]
    TimedOut,
    #[error("Unknown endpoint")]
    UnknownEndpoint,
}

impl<E> EGError<E> {
    /// Reconstructs the error, carrying the result of `f` over the external
    /// error through the [`EGError::External`] variant and preserving every
    /// non-external variant unchanged.
    pub fn map_external<F, E2>(self, f: F) -> EGError<E2>
    where
        F: FnOnce(E) -> E2,
    {
        match self {
            EGError::ApiError { code, message } => EGError::ApiError { code, message },
            EGError::BadResponse => EGError::BadResponse,
            EGError::ClockError(error) => EGError::ClockError(error),
            EGError::CryptoKey(message) => EGError::CryptoKey(message),
            EGError::External(error) => EGError::External(f(error)),
            EGError::HttpError { status, body } => EGError::HttpError { status, body },
            EGError::MutexPoisoned => EGError::MutexPoisoned,
            EGError::NotAuthenticated => EGError::NotAuthenticated,
            EGError::NotConnected => EGError::NotConnected,
            EGError::RateLimited(feedback) => EGError::RateLimited(feedback),
            EGError::TimedOut => EGError::TimedOut,
            EGError::UnknownEndpoint => EGError::UnknownEndpoint,
        }
    }
}

impl<E> EGError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// Erases the concrete external error into the default boxed form used
    /// by the connector-level API, preserving every non-external variant.
    pub fn into_boxed_external(self) -> EGError<ExternalError> {
        self.map_external(ExternalError::from)
    }
}
