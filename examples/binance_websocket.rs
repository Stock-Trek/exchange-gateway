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
    use exchange_gateway::prelude::*;
    use exchange_gateway::urls::TradingMode;
    use exchange_types::binance::{
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus,
        },
        signed::BinanceSignedParams,
        websocket::{
            BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
            BinanceWebsocketResponse, BinanceWebsocketResponseResult,
            BinanceWebsocketUnsignedParams,
        },
    };
    use std::time::Duration;

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
            println!("pushed message: {:?}", message.result);
            Ok(())
        }
    }

    pub(crate) async fn main() -> EGResult<()> {
        let connector = Connect::binance_websocket_iris(
            TradingMode::Paper,
            Clock::default(),
            PrintListener,
            iris::Config::default(),
        )?;
        connector.connect().await?;
        let response = connector
            .send(
                BinanceWebsocketRequest {
                    metadata: BinanceWebsocketMetadata {
                        id: "exchange-info".into(),
                        method: BinanceWebsocketMethodName::ExchangeInfo,
                    },
                    params: BinanceSignedParams {
                        params: BinanceWebsocketUnsignedParams::ExchangeInfo(
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
        match response.result {
            Some(BinanceWebsocketResponseResult::ExchangeInfo(info)) => {
                println!("exchangeInfo: {info:?}");
            }
            _ => println!("exchangeInfo response: {:?}", response),
        }
        connector.disconnect().await?;
        Ok(())
    }
}
