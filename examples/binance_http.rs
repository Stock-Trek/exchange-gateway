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
    use exchange_gateway::prelude::*;
    use exchange_gateway::urls::TradingMode;
    use exchange_types::binance::{
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus,
        },
        http::{BinanceHttpResponse, BinanceHttpResponseResult, BinanceHttpUnsignedRequest},
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
                let raw = serde_json::to_vec(&info).map_err(|e| {
                    EGError::External(ExternalError::from(ExampleError(e.to_string())))
                })?;
                Ok(MyResponse { raw })
            }
            BinanceHttpResponse::Failure(error) => Err(EGError::External(ExternalError::from(
                ExampleError(format!("{error:?}")),
            ))),
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
        let connector = Connect.binance_http(
            TradingMode::Paper,
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
