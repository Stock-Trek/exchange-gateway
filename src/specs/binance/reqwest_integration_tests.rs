use crate::{
    clock::Clock,
    connector::Connector,
    error::{EGError, EGResult},
    specs::binance::http::connector,
    transports::http::{HttpClientTrait, HttpRequest, HttpResponse},
};
use async_trait::async_trait;
use exchange_types::{
    binance::{
        http::{
            BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponsePayload,
            BinanceHttpResponseResult, BinanceHttpUnsignedRequest,
        },
        time::{BinanceTimeParams, BinanceTimeResult},
    },
    urls::TradingMode,
};
use std::time::Duration;

#[derive(Clone)]
struct MockHttpClient {
    clock: Clock,
}

#[async_trait]
impl HttpClientTrait for MockHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        // The origin-form request target starts with the endpoint, so a
        // `time` request is detected by its prefix.
        if message
            .query
            .as_deref()
            .is_some_and(|query| query.starts_with("time"))
        {
            // Answer with the clock's view of server time, as the real
            // exchange would.
            let body = serde_json::to_vec(&BinanceTimeResult {
                serverTime: self.clock.now_millis(),
            })
            .expect("serializing a time response should not fail");
            return Ok(HttpResponse {
                status: 200,
                body,
                headers: vec![],
            });
        }
        Err(EGError::HttpError {
            status: 404,
            body: vec![],
        })
    }
}

/// Builds an HTTP connector backed by the mock client. The mock reports the
/// given clock as the server clock on `time` responses, as the production
/// exchange does.
fn mock_http_connector(
    client_handle: std::sync::mpsc::Sender<MockHttpClient>,
    clock: Clock,
) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
    let mock_clock = clock.clone();
    let mock_client = MockHttpClient { clock: mock_clock };
    let _ = client_handle.send(mock_client.clone());
    connector(
        TradingMode::Paper,
        clock,
        Box::new(move |_url| Ok(mock_client.clone())),
    )
}

fn time_request() -> BinanceHttpRequest {
    BinanceHttpRequest {
        unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
        signature: None,
    }
}

#[tokio::test]
async fn http_connector_sync_clock_syncs_the_server_clock() {
    let (client_tx, _client_rx) = std::sync::mpsc::channel();
    let clock = Clock::default();
    let connector = mock_http_connector(client_tx, clock).unwrap();

    // Connect establishes the transport only: clock syncing is
    // user-invoked, so the clock is untouched until sync_clock is called.
    connector.connect().await.expect("connect should succeed");

    // Sync clock issues a fresh unsigned time request and adopts the
    // server clock (the mock reports the clock's view of server time).
    connector
        .sync_clock()
        .await
        .expect("sync_clock should succeed");
}

#[tokio::test]
async fn http_connector_send_returns_the_exchange_response() {
    let (client_tx, _client_rx) = std::sync::mpsc::channel();
    let clock = Clock::default();
    let connector = mock_http_connector(client_tx, clock.clone()).unwrap();

    // A time request round-trips through the factory-wired connector: the
    // mock answers with the clock's server time and the response is parsed
    // back into a BinanceHttpResponse.
    let response = connector
        .send(time_request(), Duration::from_secs(5))
        .await
        .expect("send should succeed");
    let BinanceHttpResponsePayload::Success(BinanceHttpResponseResult::Time(result)) =
        response.payload
    else {
        panic!("expected a time response");
    };
    assert!(result.serverTime >= clock.now_millis() - 1000);
}

/// A client rejecting every request with a server-side 429.
struct RejectingClient;

#[async_trait]
impl HttpClientTrait for RejectingClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        _message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        Err(EGError::RateLimited(
            crate::rate_limit::feedback::RateLimitFeedback {
                is_throttled: false,
                retry_after: None,
                usage: vec![],
            },
        ))
    }
}

#[tokio::test]
async fn http_connector_send_surfaces_a_rate_limited_rejection() {
    let connector = connector(
        TradingMode::Paper,
        Clock::default(),
        Box::new(|_url| Ok(RejectingClient)),
    )
    .unwrap();
    let result = connector.send(time_request(), Duration::from_secs(5)).await;
    assert!(matches!(result, Err(EGError::RateLimited(..))));
}
