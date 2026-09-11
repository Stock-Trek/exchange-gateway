use exchange_types::{error::ETError, new_types::UsageCount};

pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, thiserror::Error)]
pub enum EGError {
    #[cfg(feature = "auto-resync")]
    #[error("Auto resync clock thread panicked")]
    AutoResyncClockPanicked,
    #[error("Received unrecognised response")]
    BadResponse,
    #[error("A user callback panicked: {0}")]
    CallbackPanicked(String),
    #[error(
        "The server clock has not been synchronised; call sync_clock_http or sync_clock_websocket before sending signed requests"
    )]
    ClockNotSynced,
    #[error(transparent)]
    External(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(
        "HTTP request failed with status {status}: {body}",
        body = String::from_utf8_lossy(body)
    )]
    HttpError { status: u16, body: Vec<u8> },
    #[error("Failed to parse HTTP response: {source}")]
    HttpParseError {
        #[source]
        source: ETError,
    },
    #[error("Rate limiter capacity must be greater than zero")]
    InvalidRateLimitCapacity,
    #[error("Rate limiter interval must be greater than zero")]
    InvalidRateLimitInterval,
    #[cfg(feature = "auto-resync")]
    #[error("Clock sync frequency must be at least 1 minute")]
    InvalidSyncFrequency,
    #[error("Server time response did not contain a server time")]
    MissingServerTime,
    #[error("Internal mutex poisoned by a panicking operation")]
    MutexPoisoned,
    #[error("Connector is not connected")]
    NotConnected,
    #[error("The request was not sent: {0}")]
    NotSent(Box<EGError>),
    #[error("Rate limit exceeded")]
    RateLimited,
    #[error("The system time is before the UNIX epoch")]
    SystemTimeBeforeUnixEpoch,
    #[error("Request rate limit cost {cost} exceeds the maximum capacity {capacity}")]
    RequestExceedsRateLimit {
        cost: UsageCount,
        capacity: UsageCount,
    },
    #[error("Request timed out waiting for a response")]
    TimedOut,
    #[error("Connector was not initialised with a websocket listener")]
    WebsocketListenerMissing,
}
