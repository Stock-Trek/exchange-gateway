#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RateLimitType {
    RequestWeight,
    Orders,
    RawRequests,
    Connections,
}
