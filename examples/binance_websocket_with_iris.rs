//! A runnable WebSocket example that configures the underlying iris
//! websocket client through the public `iris_config` argument of
//! `Connect::binance_websocket`, instead of the `iris::Config::default()`
//! the plain `binance_websocket` example relies on. This is the websocket
//! analogue of `binance_http_with_client`: production users can tune
//! timeouts, keep-alive pings, reconnect/circuit-breaker behaviour, channel
//! buffering and TLS without forking the gateway or reaching into private
//! modules.
//!
//! ```sh
//! cargo run --example binance_websocket_with_iris --features iris
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
    use iris::{CircuitBreakerConfig, Config as IrisConfig, ServerCloseBehavior, TlsConfig};
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

    /// Builds the iris configuration passed to `Connect::binance_websocket`.
    /// Every knob on the underlying websocket client is exposed here:
    fn iris_config() -> IrisConfig {
        // Reconnect circuit breaker: how often to retry after a drop and how
        // aggressively to back off. `with_no_reconnect_limit()` removes the
        // attempt cap instead.
        let circuit_breaker = CircuitBreakerConfig::new()
            .with_max_reconnect_attempts(5)
            .with_initial_backoff(Duration::from_secs(1))
            .with_max_backoff(Duration::from_secs(30));

        // TLS: the websocket analogue of reqwest's TLS configuration. This
        // example keeps the default root store, but the full surface is
        // available, e.g. `with_ca_cert_path("ca.pem")` for a private CA,
        // `with_client_cert_paths("client.pem", "key.pem")` for mutual TLS,
        // or `with_skip_verify()` for self-signed testnet certificates.
        let tls = TlsConfig::new();

        IrisConfig::new()
            // Inbound message queue depth. A full buffer backs up the
            // connection task, so raise it for high-throughput streams.
            .with_channel_buffer_size(256)
            // Maximum time allowed for the graceful close handshake.
            .with_disconnect_timeout(Duration::from_secs(10))
            // Per-request timeout: how long a request waits for its matching
            // response before `send` reports a timeout.
            .with_message_timeout(Duration::from_secs(10))
            // Keep-alive cadence and how long a missing pong is tolerated
            // before the connection is considered dead.
            .with_ping_interval(Duration::from_secs(30))
            .with_pong_timeout(Duration::from_secs(30))
            // Reconnect (instead of disconnecting) when the server closes the
            // connection.
            .with_server_close_behavior(ServerCloseBehavior::Reconnect)
            .with_circuit_breaker_config(circuit_breaker)
            .with_tls_config(tls)
    }

    pub(crate) async fn main() -> EGResult<()> {
        let connector = Connect.binance_websocket(
            TradingMode::Paper,
            to_unsigned_request,
            to_external_response,
            PrintListener,
            None,
            Clock::default(),
            false,
            iris_config(),
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
