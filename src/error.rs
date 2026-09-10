use exchange_types::error::ETError;

pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
#[error(
    "Failed to parse HTTP response: {source}; body: {body}",
    body = String::from_utf8_lossy(body)
)]
pub struct HttpParseError {
    #[source]
    pub source: ETError,
    pub body: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[error("Received unrecognised response")]
    BadResponse,
    #[cfg(feature = "auto-resync")]
    #[error("Auto resync clock thread panicked")]
    AutoResyncClockPanicked,
    #[error("A user callback panicked: {0}")]
    CallbackPanicked(String),
    #[error(transparent)]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(
        "HTTP request failed with status {status}: {body}",
        body = String::from_utf8_lossy(body)
    )]
    HttpError { status: u16, body: Vec<u8> },
    #[error("Internal mutex poisoned by a panicking operation")]
    MutexPoisoned,
    #[error("Connector is not connected")]
    NotConnected,
    #[error("The request was not sent: {0}")]
    NotSent(Box<EGError>),
    #[error("Rate limit exceeded")]
    RateLimited,
    #[error("Request timed out waiting for a response")]
    TimedOut,
}
