use std::time::Duration;

/// Server-reported usage of a single rate-limit bucket.
///
/// Exchanges expose their *actual* usage and limits in responses: Binance
/// returns `X-MBX-USED-WEIGHT-1M` / `X-MBX-ORDER-COUNT-*` headers on REST
/// responses, a `rateLimits` array on WebSocket API responses, and the current
/// limit definitions in `exchangeInfo`. Because those values are dynamic (the
/// `exchangeInfo` weight and the configured limits change without notice),
/// hard-coded local weights can drift. Applying this feedback to the local
/// limiter keeps the model aligned with the server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimitUsage {
    /// The bucket interval in nanoseconds (e.g. one minute for request weight).
    pub interval_nanos: u128,
    /// Usage reported by the server within the interval.
    pub used: u32,
    /// The limit reported by the server for the interval, when known.
    ///
    /// Binance's usage headers omit the limit; the `rateLimits` arrays in
    /// `exchangeInfo` and WebSocket responses include it. When it is `None`
    /// the local bucket keeps its configured limit and only trims remaining
    /// capacity to `limit - used` (never adding capacity).
    pub limit: Option<u32>,
}

/// Server-side rate-limit feedback collected from a response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RateLimitFeedback {
    /// Usage observed by the server per rate-limit bucket.
    pub usage: Vec<RateLimitUsage>,
    /// Seconds the server asked us to wait before retrying (429/418
    /// `Retry-After` header).
    pub retry_after: Option<Duration>,
    /// The server rejected the request with 429 (too many requests) or 418
    /// (IP auto-banned). Local limiters are drained until `retry_after`
    /// elapses (or a short default when the header is absent).
    pub throttled: bool,
}
