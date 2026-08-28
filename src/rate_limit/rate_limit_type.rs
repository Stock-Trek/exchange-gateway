/// The kind of rate limit a bucket models, mirroring the `rateLimitType`
/// exchanges report in their rate-limit feedback (e.g. Binance's
/// `REQUEST_WEIGHT` / `RAW_REQUESTS` / `ORDERS` / `CONNECTIONS`).
///
/// Feedback is matched to buckets by type *and* interval: Binance reports
/// `REQUEST_WEIGHT` and `RAW_REQUESTS` with the same one-minute window but
/// different limits (6000 vs 61000), so matching on interval alone would let
/// the wrong usage overwrite the weight bucket's capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitType {
    /// Request weight: the per-request cost shared by all request types.
    RequestWeight,
    /// Order count: the number of orders placed (per account).
    Orders,
    /// Raw request count: every request regardless of weight. Not modelled
    /// locally, so usage of this type is never applied to a local bucket.
    RawRequests,
    /// Number of connections (e.g. WebSocket). Not modelled locally, so
    /// usage of this type is never applied to a local bucket.
    Connections,
}
