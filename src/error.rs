use crate::rate_limit::feedback::RateLimitFeedback;
use std::time::SystemTimeError;

pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[error("Exchange API error {code}: {message}")]
    ApiError { code: i64, message: String },
    #[error("Received unrecognised response")]
    BadResponse,
    #[error("Clock error: {0}")]
    ClockError(SystemTimeError),
    #[error("Crypto key error: {0}")]
    CryptoKey(String),
    #[error(transparent)]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
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
    #[error("The request was not sent: {0}")]
    NotSent(Box<EGError>),
    #[error("Rate limit exceeded")]
    RateLimited(RateLimitFeedback),
    #[error("Request timed out waiting for a response")]
    TimedOut,
    #[error("Unknown endpoint")]
    UnknownEndpoint,
}
