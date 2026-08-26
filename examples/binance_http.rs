//! A minimal, runnable REST example: polls `GET /api/v3/exchangeInfo` on the
//! Binance testnet through the gateway's HTTP connector.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example binance_http --features reqwest,serde
//! ```
//!
//! The example needs no credentials because `exchangeInfo` is a public
//! (MARKET_DATA) endpoint. Signed requests would additionally require
//! [`ApiKeyCredentials`]

#[cfg(not(all(feature = "reqwest", feature = "serde")))]
fn main() {}

#[cfg(all(feature = "reqwest", feature = "serde"))]
#[tokio::main]
async fn main() -> exchange_gateway::error::EGResult<()> {
    binance::main().await
}

#[cfg(all(feature = "reqwest", feature = "serde"))]
mod binance {
    use exchange_gateway::prelude::*;
    use exchange_gateway::urls::TradingMode;
    use exchange_types::binance::{
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus,
        },
        http::{
            BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
            BinanceHttpUnsignedRequest,
        },
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

    #[derive(Debug, Clone)]
    struct MyListener;

    #[async_trait::async_trait]
    impl ListenerTrait for MyListener {
        type TMessage = MyResponse;
        async fn on_message(&self, message: MyResponse) -> EGResult<()> {
            println!("exchangeInfo: {} bytes", message.raw.len());
            Ok(())
        }
    }

    /// A local error so that exchange-types errors can be boxed into [`EGError`]
    /// (the upstream `BinanceError` does not implement `std::error::Error`).
    #[derive(Debug)]
    struct ExampleError(String);

    impl std::fmt::Display for ExampleError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for ExampleError {}

    /// Converts the gateway's typed unsigned request into the exchange-types
    /// request. Requests that carry no signature payload (`exchangeInfo`) are
    /// sent unsigned.
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

    /// Converts the gateway's typed request into a transport-level request.
    ///
    /// The gateway only validates that the endpoint is known; the transport
    /// request itself is entirely yours to build. For signed requests the query
    /// string must exactly match what the gateway signed.
    fn to_transport_request(request: BinanceHttpRequest) -> EGResult<HttpRequest> {
        let (_endpoint, query) = match request.params {
            BinanceHttpUnsignedRequest::ExchangeInfo(_) => ("exchangeInfo", None),
            _ => return Err(EGError::UnknownEndpoint),
        };
        Ok(HttpRequest {
            method: reqwest::Method::GET,
            query: query.map(str::to_string),
            headers: vec![],
            body: None,
        })
    }

    /// Converts a transport-level response into the exchange-types response.
    ///
    /// The transport rejects non-2xx HTTP statuses (4xx/429/5xx are returned
    /// as [`EGError`]s), so only successful responses reach this converter.
    fn to_binance_response(response: HttpResponse) -> EGResult<BinanceHttpResponse> {
        Ok(BinanceHttpResponse::Result(
            serde_json::from_slice(&response.body)
                .map_err(|e| EGError::External(Box::new(ExampleError(e.to_string()))))?,
        ))
    }

    fn to_external_response(response: BinanceHttpResponse) -> EGResult<MyResponse> {
        match response {
            BinanceHttpResponse::Result(BinanceHttpResponseResult::ExchangeInfo(info)) => {
                let raw = serde_json::to_vec(&info)
                    .map_err(|e| EGError::External(Box::new(ExampleError(e.to_string()))))?;
                Ok(MyResponse { raw })
            }
            BinanceHttpResponse::Error(error) => Err(EGError::External(Box::new(ExampleError(
                format!("{error:?}"),
            )))),
            BinanceHttpResponse::Result(BinanceHttpResponseResult::SpotOrder(_)) => {
                Err(EGError::UnknownEndpoint)
            }
            BinanceHttpResponse::Result(BinanceHttpResponseResult::AssetLimits(_)) => {
                Err(EGError::UnknownEndpoint)
            }
        }
    }

    pub(crate) async fn main() -> EGResult<()> {
        let listener: Arc<dyn ListenerTrait<TMessage = MyResponse>> = Arc::new(MyListener);

        let connector = Connect.binance_http(
            TradingMode::Paper,
            ReqwestHttpClient::new,
            Arc::new(to_unsigned_request),
            Arc::new(to_transport_request),
            Arc::new(to_binance_response),
            Arc::new(to_external_response),
            listener,
            None,
        )?;
        connector.connect().await?;

        // `send` is fire-and-forget: the exchange's reply is delivered to the
        // listener (`MyListener`) asynchronously.
        connector
            .send(
                MyRequest {
                    exchange_info: true,
                },
                false,
                Duration::from_secs(10),
            )
            .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        connector.disconnect().await?;
        Ok(())
    }
}
