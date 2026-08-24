#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use strum::Display;

#[allow(non_snake_case)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BinanceRateLimit {
    pub count: i64,
    pub interval: BinanceRateLimitInterval,
    pub intervalNum: i32,
    pub limit: i64,
    pub rateLimitType: BinanceRateLimitType,
}

#[allow(non_camel_case_types)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, Display)]
pub enum BinanceRateLimitInterval {
    DAY,
    HOUR,
    MINUTE,
    SECOND,
}

#[allow(non_camel_case_types)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy, Display)]
pub enum BinanceRateLimitType {
    CONNECTIONS,
    ORDERS,
    REQUEST_WEIGHT,
}
