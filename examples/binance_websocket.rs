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
        websocket::{
            BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketResponse,
            BinanceWebsocketResponseResult, BinanceWebsocketUnsignedParams,
            BinanceWebsocketUnsignedRequest,
        },
    };
    use std::time::Duration;

    #[derive(Debug, Clone)]
    struct MyRequest {
        exchange_info: bool,
    }

    #[derive(Debug, Clone)]
    struct MyResponse {
        raw: Vec<u8>,
    }

    #[derive(Debug)]
    struct ExampleError(String);

    impl std::fmt::Display for ExampleError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for ExampleError {}

    /// Observes connection lifecycle and any messages the exchange pushes
    /// without a matching `send` waiter (the `send` response itself is
    /// returned directly to the caller instead).
    struct PrintListener;

    #[async_trait::async_trait]
    impl ListenerTrait for PrintListener {
        type TMessage = MyResponse;

        async fn on_connected(&self) -> EGResult<()> {
            println!("websocket connected");
            Ok(())
        }

        async fn on_disconnected(&self) -> EGResult<()> {
            println!("websocket disconnected");
            Ok(())
        }

        async fn on_message(&self, message: MyResponse) -> EGResult<()> {
            println!("pushed message: {} bytes", message.raw.len());
            Ok(())
        }
    }

    fn to_unsigned_request(request: MyRequest) -> EGResult<BinanceWebsocketUnsignedRequest> {
        if request.exchange_info {
            Ok(BinanceWebsocketUnsignedRequest {
                metadata: BinanceWebsocketMetadata {
                    id: "exchange-info".into(),
                    method: BinanceWebsocketMethodName::ExchangeInfo,
                },
                params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                    permissions: vec![BinanceExchangeInfoPermission::SPOT],
                    symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
                }),
            })
        } else {
            Err(EGError::UnknownEndpoint)
        }
    }

    fn to_external_response(response: BinanceWebsocketResponse) -> EGResult<MyResponse> {
        match response.result {
            Some(BinanceWebsocketResponseResult::ExchangeInfo(info)) => {
                let raw = serde_json::to_vec(&info)
                    .map_err(|e| EGError::External(Box::new(ExampleError(e.to_string()))))?;
                Ok(MyResponse { raw })
            }
            _ => Err(EGError::UnknownEndpoint),
        }
    }

    pub(crate) async fn main() -> EGResult<()> {
        let connector = Connect::binance_websocket_iris(
            TradingMode::Paper,
            to_unsigned_request,
            to_external_response,
            PrintListener,
            None,
            Clock::default(),
            false,
            Duration::from_secs(20),
            iris::Config::default(),
        )?;
        connector.connect().await?;
        let response = connector
            .send(
                MyRequest {
                    exchange_info: true,
                },
                false,
                Duration::from_secs(10),
            )
            .await?;
        println!("exchangeInfo: {} bytes", response.raw.len());
        connector.disconnect().await?;
        Ok(())
    }
}
