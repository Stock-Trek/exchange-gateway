//! A minimal, runnable REST example: polls `GET /api/v3/exchangeInfo` on the
//! Binance testnet through the gateway's HTTP connector. `send` is a
//! send-and-wait call: it returns the exchange's response directly, so no
//! listener is needed to observe it.
//!
//! ```sh
//! cargo run --example binance_http --features reqwest
//! ```

#[cfg(not(feature = "reqwest"))]
fn main() {}

#[cfg(feature = "reqwest")]
#[tokio::main]
async fn main() -> exchange_gateway::error::EGResult<()> {
    binance::main().await
}

#[cfg(feature = "reqwest")]
mod binance {
    use exchange_gateway::{prelude::*, rate_limiter::RateLimiter};
    use exchange_types::{
        binance::{
            exchange_info::{
                BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
                BinanceExchangeInfoSymbolStatus,
            },
            http::{BinanceHttpRequest, BinanceHttpUnsignedRequest},
        },
        urls::TradingMode,
    };
    use std::{sync::Arc, time::Duration};

    #[derive(Debug)]
    struct AcceptingRateLimiter;

    impl RateLimiter for AcceptingRateLimiter {
        fn did_acquire(
            &self,
            _limit_costs: &Vec<(exchange_types::rate_limited::RateLimitType, u32)>,
        ) -> bool {
            true
        }
    }

    pub(crate) async fn main() -> EGResult<()> {
        let connector = Connect::binance_http_reqwest(
            TradingMode::Paper,
            Clock::default(),
            Arc::new(AcceptingRateLimiter),
        )?;
        connector.connect().await?;
        let response = connector
            .send(
                BinanceHttpRequest {
                    unsigned: BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
                        permissions: vec![BinanceExchangeInfoPermission::SPOT],
                        symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
                    }),
                    signature: None,
                },
                Duration::from_secs(10),
            )
            .await?;
        println!("exchangeInfo: {:?}", response);
        connector.disconnect().await?;
        Ok(())
    }
}
