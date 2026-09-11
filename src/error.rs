use exchange_types::{
    error::ETError,
    new_types::{Nanoseconds, UsageCount},
};

pub type EGResult<T> = Result<T, EGError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendFailure {
    Failed,
    NotSent,
    Unknown,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EGError {
    #[cfg(feature = "auto-resync")]
    #[error("Auto resync clock task panicked")]
    AutoResyncClockPanicked,
    #[cfg(feature = "auto-resync")]
    #[error("Auto resync clock task is no longer running")]
    AutoResyncClockStopped,
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
    #[error("Failed to parse websocket response: {source}")]
    WebsocketParseError {
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
    #[error("{source}")]
    Send {
        failure: SendFailure,
        #[source]
        source: Box<EGError>,
    },
    #[error("Rate limit exceeded")]
    RateLimited,
    #[error("The system time is before the UNIX epoch")]
    SystemTimeBeforeUnixEpoch,
    #[error(
        "Request rate limit cost {cost} exceeds the maximum capacity {capacity} for interval {interval_nanos}ns"
    )]
    RequestExceedsRateLimit {
        cost: UsageCount,
        capacity: UsageCount,
        interval_nanos: Nanoseconds,
    },
    #[error("Request timed out waiting for a response")]
    TimedOut,
    #[error("Connector was not initialised with a websocket listener")]
    WebsocketListenerMissing,
}

impl EGError {
    pub(crate) fn external(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        EGError::External(Box::new(source))
    }
    pub(crate) fn send_not_sent(source: EGError) -> Self {
        EGError::Send {
            failure: SendFailure::NotSent,
            source: Box::new(source),
        }
    }
    pub(crate) fn send_not_sent_external(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        EGError::Send {
            failure: SendFailure::NotSent,
            source: Box::new(EGError::external(source)),
        }
    }
    pub(crate) fn send_failed(source: EGError) -> Self {
        EGError::Send {
            failure: SendFailure::Failed,
            source: Box::new(source),
        }
    }
    pub(crate) fn send_unknown(source: EGError) -> Self {
        EGError::Send {
            failure: SendFailure::Unknown,
            source: Box::new(source),
        }
    }
    pub(crate) fn send_unknown_external(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        EGError::Send {
            failure: SendFailure::Unknown,
            source: Box::new(EGError::external(source)),
        }
    }
}
