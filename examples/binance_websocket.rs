//! A minimal, runnable WebSocket example: opens a connection to the Binance
//! testnet through the gateway's WebSocket connector and polls
//! `exchangeInfo`. `send` is a send-and-wait call: it returns the exchange's
//! response directly, while the listener observes connection lifecycle and
//! any other (e.g. push) messages the exchange sends.
//!
//! ```sh
//! cargo run --example binance_websocket --features iris
//! ```
//!
//! The example installs the rustls `ring` crypto provider up front because
//! the example binary links both the `ring` (WebSocket transport) and
//! `aws-lc-rs` (HTTP transport) providers; a plain library user who enables
//! only `iris` does not need to do this.

#[cfg(not(feature = "iris"))]
fn main() {}

#[cfg(feature = "iris")]
#[tokio::main]
async fn main() -> exchange_gateway::error::EGResult<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    binance::main().await
}

#[cfg(feature = "iris")]
mod binance {
    use exchange_gateway::{prelude::*, rate_limiter::RateLimiter};
    use exchange_types::{
        binance::{
            exchange_info::{
                BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
                BinanceExchangeInfoSymbolStatus,
            },
            websocket::{
                BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketSignedParams,
                BinanceWebsocketUnsignedParams,
            },
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

    /// Observes connection lifecycle and any messages the exchange pushes
    /// without a matching `send` waiter (the `send` response itself is
    /// returned directly to the caller instead).
    struct PrintListener;

    #[async_trait::async_trait]
    impl ListenerTrait for PrintListener {
        type TMessage = BinanceWebsocketResponse;

        async fn on_connected(&self) -> EGResult<()> {
            println!("websocket connected");
            Ok(())
        }

        async fn on_disconnected(&self) -> EGResult<()> {
            println!("websocket disconnected");
            Ok(())
        }

        async fn on_message(&self, message: BinanceWebsocketResponse) -> EGResult<()> {
            println!("pushed message: {:?}", message);
            Ok(())
        }
    }

    pub(crate) async fn main() -> EGResult<()> {
        let connector = Connect::binance_websocket_iris(
            TradingMode::Paper,
            Clock::default(),
            Arc::new(AcceptingRateLimiter),
            PrintListener,
            iris::Config::default(),
        )?;
        connector.connect().await?;
        let response = connector
            .send(
                BinanceWebsocketRequest {
                    id: "id".into(),
                    params: BinanceWebsocketSignedParams {
                        unsigned: BinanceWebsocketUnsignedParams::ExchangeInfo(
                            BinanceExchangeInfoParams {
                                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
                            },
                        ),
                        signature: None,
                    },
                },
                Duration::from_secs(10),
            )
            .await?;
        println!("exchangeInfo: {:?} bytes", response);
        connector.disconnect().await?;
        Ok(())
    }
}
