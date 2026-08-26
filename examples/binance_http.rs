//! Minimal end-to-end example: fetch `exchangeInfo` from the Binance testnet
//! through the REST connector.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example binance_http --features reqwest,iris,serde
//! ```

#[cfg(not(all(feature = "reqwest", feature = "iris", feature = "serde")))]
fn main() {}

#[cfg(all(feature = "reqwest", feature = "iris", feature = "serde"))]
mod run {
    use exchange_gateway::prelude::*;
    use exchange_gateway::urls::TradingMode;
    use exchange_types::binance::exchange_info::{
        BinanceExchangeInfoParams, BinanceExchangeInfoPermission, BinanceExchangeInfoSymbolStatus,
    };
    use exchange_types::binance::http::{
        BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
        BinanceHttpUnsignedRequest,
    };
    use std::sync::Arc;
    use std::time::Duration;

    struct PrintingListener;

    #[async_trait::async_trait]
    impl ListenerTrait for PrintingListener {
        type TMessage = String;

        async fn on_message(&self, message: String) -> EGResult<()> {
            println!("{message}");
            Ok(())
        }
    }

    fn to_unsigned_request(request: String) -> EGResult<BinanceHttpUnsignedRequest> {
        match request.as_str() {
            "exchangeInfo" => Ok(BinanceHttpUnsignedRequest::ExchangeInfo(
                BinanceExchangeInfoParams {
                    permissions: vec![BinanceExchangeInfoPermission::SPOT],
                    symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
                },
            )),
            _ => Err(EGError::BadResponse),
        }
    }

    fn to_transport_request(request: BinanceHttpRequest) -> EGResult<HttpRequest> {
        let (method, query) = match request.params {
            BinanceHttpUnsignedRequest::ExchangeInfo(params) => (
                reqwest::Method::GET,
                format!(
                    "permissions={}&symbolStatus={}",
                    params.permissions[0], params.symbolStatus
                ),
            ),
            BinanceHttpUnsignedRequest::AssetLimits => (reqwest::Method::GET, String::new()),
            BinanceHttpUnsignedRequest::SpotOrderRequest(_) => {
                (reqwest::Method::POST, String::new())
            }
        };
        Ok(HttpRequest {
            method,
            query: Some(query),
            headers: Vec::new(),
            body: None,
        })
    }

    fn to_binance_response(response: HttpResponse) -> EGResult<BinanceHttpResponse> {
        if !(200..300).contains(&response.status) {
            return Err(EGError::External(Box::new(std::io::Error::other(format!(
                "exchange returned HTTP status {}",
                response.status
            )))));
        }
        serde_json::from_slice(&response.body).map_err(|error| EGError::External(Box::new(error)))
    }

    fn to_external_response(response: BinanceHttpResponse) -> EGResult<String> {
        match response {
            BinanceHttpResponse::Result(BinanceHttpResponseResult::ExchangeInfo(info)) => {
                Ok(format!(
                    "exchangeInfo: {} symbols, serverTime {}",
                    info.symbols.len(),
                    info.serverTime
                ))
            }
            BinanceHttpResponse::Error(error) => Err(EGError::External(Box::new(
                std::io::Error::other(format!("exchange error {}: {}", error.code, error.msg)),
            ))),
            other => Ok(format!("response: {other:?}")),
        }
    }

    pub async fn main() {
        let listener: Arc<dyn ListenerTrait<TMessage = String>> = Arc::new(PrintingListener);
        let connector = Connect.binance_http(
            TradingMode::Paper,
            ReqwestHttpClient::new,
            Arc::new(to_unsigned_request),
            Arc::new(to_transport_request),
            Arc::new(to_binance_response),
            Arc::new(to_external_response),
            listener,
            None,
        );
        connector.connect().await.expect("connect should succeed");
        connector
            .send("exchangeInfo".to_string(), false, Duration::from_secs(10))
            .await
            .expect("send should succeed");
    }
}

#[cfg(all(feature = "reqwest", feature = "iris", feature = "serde"))]
#[tokio::main]
async fn main() {
    run::main().await;
}
