use crate::{
    error::EGResult,
    rate_limit::{
        feedback::RateLimitUsage, rate_limit_config::RateLimitConfig,
        rate_limit_type::RateLimitType, rate_limiter::RateLimiter, rate_limits::RateLimits,
    },
    sign::encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
    time_sync::TimeSync,
    urls::{ExchangeTransportUrls, ExchangeUrls},
};
use exchange_types::binance::{
    rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
    spot::BinanceSpotOrderParams,
};
use rust_decimal::Decimal;
use secrecy::SecretString;
use std::time::Duration;
use uuid::Uuid;

pub(crate) const DEFAULT_RECV_WINDOW_MILLIS: u64 = 5000;

pub(crate) fn exchange_urls() -> ExchangeUrls {
    ExchangeUrls::new(
        "BINANCE",
        ExchangeTransportUrls::new(
            "https://api.binance.com/api/v3",
            "https://testnet.binance.vision/api/v3",
        ),
        ExchangeTransportUrls::new(
            "wss://ws-api.binance.com:443/ws-api/v3",
            "wss://ws-api.testnet.binance.vision:443/ws-api/v3",
        ),
    )
}

pub(crate) fn sync_timestamp_fields(
    timestamp: &mut i64,
    recv_window: &mut Option<Decimal>,
    time_sync: &TimeSync,
) {
    *timestamp = time_sync.now_millis();
    if recv_window.is_none() {
        *recv_window = Some(Decimal::from(DEFAULT_RECV_WINDOW_MILLIS));
    }
}

pub(crate) fn data_signer(secret: &SecretString) -> EGResult<DataSigner> {
    SigningAlgorithm::HmacSha256.signer(secret)
}

pub(crate) fn rate_limits() -> RateLimits {
    RateLimits {
        weight: RateLimiter::new(vec![RateLimitConfig {
            rate_limit_type: RateLimitType::RequestWeight,
            capacity_per_interval: 6000,
            interval_nanos: Duration::from_mins(1).as_nanos(),
        }]),
        orders: RateLimiter::new(vec![
            RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 50,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            },
            RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 160_000,
                interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
            },
        ]),
    }
}

pub(crate) fn rate_limit_usage(limit: &BinanceRateLimit) -> Option<RateLimitUsage> {
    let interval_nanos = rate_limit_interval_nanos(limit.interval)? * limit.intervalNum as u128;
    Some(RateLimitUsage {
        rate_limit_type: rate_limit_type(limit.rateLimitType),
        interval_nanos,
        used: limit.count.map(|count| count.max(0) as u32),
        limit: Some(limit.limit.max(0) as u32),
    })
}

pub(crate) fn rate_limit_type(rate_limit_type: BinanceRateLimitType) -> RateLimitType {
    match rate_limit_type {
        BinanceRateLimitType::CONNECTIONS => RateLimitType::Connections,
        BinanceRateLimitType::ORDERS => RateLimitType::Orders,
        BinanceRateLimitType::RAW_REQUESTS => RateLimitType::RawRequests,
        BinanceRateLimitType::REQUEST_WEIGHT => RateLimitType::RequestWeight,
    }
}

pub(crate) fn rate_limit_interval_nanos(interval: BinanceRateLimitInterval) -> Option<u128> {
    let secs = match interval {
        BinanceRateLimitInterval::SECOND => 1,
        BinanceRateLimitInterval::MINUTE => 60,
        BinanceRateLimitInterval::HOUR => 60 * 60,
        BinanceRateLimitInterval::DAY => 24 * 60 * 60,
    };
    Some(Duration::from_secs(secs).as_nanos())
}

pub(crate) fn order_weight(params: &BinanceSpotOrderParams) -> u32 {
    if params.icebergQty.is_some()
        || params.trailingDelta.is_some()
        || params.pegPriceType.is_some()
        || params.pegOffsetValue.is_some()
    {
        2
    } else {
        1
    }
}

pub(crate) fn id() -> String {
    Uuid::new_v4().to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    fn rate_limit(
        rate_limit_type: BinanceRateLimitType,
        interval: BinanceRateLimitInterval,
        interval_num: i32,
        limit: i64,
        count: Option<i64>,
    ) -> BinanceRateLimit {
        BinanceRateLimit {
            count,
            interval,
            intervalNum: interval_num,
            limit,
            rateLimitType: rate_limit_type,
        }
    }

    #[test]
    fn rate_limit_usage_maps_binance_intervals_and_types() {
        let usage = rate_limit_usage(&rate_limit(
            BinanceRateLimitType::ORDERS,
            BinanceRateLimitInterval::DAY,
            1,
            160_000,
            Some(10),
        ))
        .unwrap();
        assert_eq!(usage.rate_limit_type, RateLimitType::Orders);
        assert_eq!(
            usage.interval_nanos,
            Duration::from_secs(24 * 60 * 60).as_nanos()
        );
        assert_eq!(usage.used, Some(10));
        assert_eq!(usage.limit, Some(160_000));
        assert_eq!(
            rate_limit_usage(&rate_limit(
                BinanceRateLimitType::RAW_REQUESTS,
                BinanceRateLimitInterval::MINUTE,
                1,
                61000,
                Some(6000),
            ))
            .unwrap()
            .rate_limit_type,
            RateLimitType::RawRequests
        );
        assert!(
            rate_limit_usage(&rate_limit(
                BinanceRateLimitType::ORDERS,
                BinanceRateLimitInterval::MINUTE,
                1,
                10,
                None,
            ))
            .is_some()
        );
    }

    #[test]
    fn weight_rate_limit_is_6000_per_minute() {
        let limits = rate_limits();
        for _ in 0..300 {
            assert!(limits.weight.did_acquire(20).unwrap());
        }
        assert!(!limits.weight.did_acquire(1).unwrap());
        limits.weight.refund(20).unwrap();
        assert!(limits.weight.did_acquire(1).unwrap());
    }

    #[test]
    fn order_rate_limit_is_50_per_10_seconds() {
        let limits = rate_limits();
        for _ in 0..50 {
            assert!(limits.orders.did_acquire(1).unwrap());
        }
        assert!(!limits.orders.did_acquire(1).unwrap());
        limits.orders.refund(1).unwrap();
        assert!(limits.orders.did_acquire(1).unwrap());
    }
}
