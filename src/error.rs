pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[error("Received unrecognised response")]
    BadResponse,
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
    #[error("Unknown endpoint")]
    UnknownEndpoint,
    #[error("Rate limit exceeded")]
    RateLimited,
    #[error("Request timed out waiting for a response")]
    TimedOut,
}
