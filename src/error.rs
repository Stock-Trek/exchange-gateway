use crate::rate_limit::feedback::RateLimitFeedback;

pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[error("Received unrecognised response")]
    BadResponse,
    #[error("Exchange API error {code}: {message}")]
    ApiError { code: i64, message: String },
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
    RateLimited {
        /// Server-side rate-limit feedback observed on the rejected response
        /// (e.g. Binance's 429/418 with a `Retry-After` header and `X-MBX-*`
        /// usage headers). Callers feed this back into the local limiter so
        /// the local model stays aligned with the server. Empty when the
        /// request was rejected by a *local* limiter.
        feedback: RateLimitFeedback,
    },
    #[error("Request timed out waiting for a response")]
    TimedOut,
}
