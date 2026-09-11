use exchange_types::{
    error::ETError,
    new_types::{Nanoseconds, UsageCount},
};

pub type EGResult<T> = Result<T, EGError>;

/// What happened to a request that was handed to a send path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RequestOutcome {
    /// A valid response was received.
    Succeeded,
    /// The exchange received the request and rejected it; it did not take effect.
    Failed,
    /// The request definitely never left the client.
    NotSent,
    /// The request may have been sent; the outcome is unknown.
    Unknown,
}

/// A send failure with the transport outcome attached, so callers never have
/// to infer "definitely not sent" vs "possibly executed" from an error variant.
#[derive(Debug)]
pub struct SendFailure {
    pub outcome: RequestOutcome,
    pub source: Box<EGError>,
}

impl SendFailure {
    pub fn new(outcome: RequestOutcome, source: EGError) -> Self {
        Self {
            outcome,
            source: Box::new(source),
        }
    }
    pub fn failed(source: EGError) -> Self {
        Self::new(RequestOutcome::Failed, source)
    }
    pub fn not_sent(source: EGError) -> Self {
        Self::new(RequestOutcome::NotSent, source)
    }
    pub fn unknown(source: EGError) -> Self {
        Self::new(RequestOutcome::Unknown, source)
    }
    pub fn into_source(self) -> EGError {
        *self.source
    }
}

impl std::fmt::Display for SendFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for SendFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
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
    #[error(transparent)]
    Send(#[from] SendFailure),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn io_error() -> EGError {
        EGError::External(Box::new(std::io::Error::other("boom")))
    }

    #[test]
    fn send_failure_constructors_set_the_outcome() {
        assert_eq!(
            SendFailure::not_sent(io_error()).outcome,
            RequestOutcome::NotSent
        );
        assert_eq!(
            SendFailure::failed(io_error()).outcome,
            RequestOutcome::Failed
        );
        assert_eq!(
            SendFailure::unknown(io_error()).outcome,
            RequestOutcome::Unknown
        );
    }

    #[test]
    fn send_failure_proxies_display_and_source_to_the_cause() {
        let failure = SendFailure::not_sent(io_error());
        assert_eq!(failure.to_string(), io_error().to_string());
        assert!(std::error::Error::source(&failure).is_some());
    }

    #[test]
    fn converting_a_send_failure_preserves_the_outcome() {
        let error: EGError = SendFailure::failed(EGError::RateLimited).into();
        match error {
            EGError::Send(failure) => assert_eq!(failure.outcome, RequestOutcome::Failed),
            other => panic!("expected EGError::Send, got {other:?}"),
        }
    }
}
