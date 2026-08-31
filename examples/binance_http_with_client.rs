//! A runnable REST example that injects a custom `reqwest::Client` (here
//! configured with a 10s timeout and a custom user-agent) through the public
//! `Connect::binance_http_with_client` entry point, instead of the default
//! `reqwest::Client::new()`. The same mechanism supports proxies, TLS
//! configuration, custom headers and any other `reqwest::Client` setting,
//! or a fully custom [`HttpClientTrait`] implementation.
//!
//! ```sh
//! cargo run --example binance_http_with_client --features reqwest
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
    use exchange_gateway::prelude::*;
    use exchange_gateway::{
        specs::binance::common::exchange_urls, transports::reqwest::ReqwestHttpClient,
    };
    use exchange_types::binance::{
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus,
        },
        http::{BinanceHttpResponse, BinanceHttpResponseResult, BinanceHttpUnsignedRequest},
    };
    use std::{sync::Arc, time::Duration};

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

    fn to_unsigned_request(request: MyRequest) -> EGResult<BinanceHttpUnsignedRequest> {
        if request.exchange_info {
            Ok(BinanceHttpUnsignedRequest::ExchangeInfo(
                BinanceExchangeInfoParams {
                    permissions: vec![BinanceExchangeInfoPermission::SPOT],
                    symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
                },
            ))
        } else {
            Err(EGError::UnknownEndpoint)
        }
    }

    fn to_external_response(response: BinanceHttpResponse) -> EGResult<MyResponse> {
        match response {
            BinanceHttpResponse::Success(BinanceHttpResponseResult::ExchangeInfo(info)) => {
                let raw = serde_json::to_vec(&info)
                    .map_err(|e| EGError::External(Box::new(ExampleError(e.to_string()))))?;
                Ok(MyResponse { raw })
            }
            BinanceHttpResponse::Failure(error) => Err(EGError::External(Box::new(ExampleError(
                format!("{error:?}"),
            )))),
            BinanceHttpResponse::Success(BinanceHttpResponseResult::SpotOrder(_)) => {
                Err(EGError::UnknownEndpoint)
            }
            BinanceHttpResponse::Success(BinanceHttpResponseResult::AssetLimits(_)) => {
                Err(EGError::UnknownEndpoint)
            }
            BinanceHttpResponse::Success(BinanceHttpResponseResult::AmendOrder(_))
            | BinanceHttpResponse::Success(BinanceHttpResponseResult::CancelAllOrders(_))
            | BinanceHttpResponse::Success(BinanceHttpResponseResult::CancelOrder(_))
            | BinanceHttpResponse::Success(BinanceHttpResponseResult::Time(_)) => {
                Err(EGError::UnknownEndpoint)
            }
        }
    }

    pub(crate) async fn main() -> EGResult<()> {
        // Custom reqwest client: proxies, TLS, timeouts, headers, ... Any
        // reqwest::Client::builder() setting is available here.
        let custom_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("exchange-gateway-example/0.1")
            .build()
            .map_err(|e| EGError::External(Box::new(ExampleError(e.to_string()))))?;

        // Resolve the canonical base URL (honours BINANCE_HTTP_PAPER /
        // BINANCE_HTTP_REAL env-var overrides, like the default path) and
        // wrap the custom client.
        let url = exchange_urls().url(ExchangeTransportType::Http, TradingMode::Paper);
        let client = Arc::new(ReqwestHttpClient::with_client(&url, custom_client));

        let connector = Connect.binance_http_with_client(
            client,
            to_unsigned_request,
            to_external_response,
            None,
            Clock::default(),
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
