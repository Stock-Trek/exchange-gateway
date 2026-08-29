use crate::{
    clock::Clock,
    connector::Connector,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::ArcTryConvertValue,
    listeners::listener::ListenerTrait,
    rate_limit::{
        feedback::RateLimitFeedback, rate_limit_config::RateLimitConfig,
        rate_limit_type::RateLimitType, rate_limiter::RateLimiter, rate_limits::RateLimits,
    },
    specs::binance::{common::rate_limits, http::connector_with_client},
    transports::{
        http::HttpClientTrait,
        reqwest::{HttpRequest, HttpResponse},
    },
};
use async_trait::async_trait;
use exchange_types::binance::{
    asset_limits::BinanceAssetLimitsParams,
    exchange_info::BinanceOrderType,
    http::{BinanceHttpResponse, BinanceHttpUnsignedRequest},
    spot::{
        BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
        BinanceSpotOrderParams, BinanceTimeInForce,
    },
};
use secrecy::SecretString;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

fn spot_order_params() -> BinanceSpotOrderParams {
    BinanceSpotOrderParams {
        apiKey: Some("my-api-key".into()),
        icebergQty: None,
        newClientOrderId: "abc".into(),
        newOrderRespType: BinanceNewOrderResponseType::ACK,
        pegPriceType: None,
        pegOffsetValue: None,
        pegOffsetType: None,
        price: Some("100".parse().unwrap()),
        quantity: Some("1".parse().unwrap()),
        quoteOrderQty: None,
        recvWindow: None,
        selfTradePreventionMode: BinanceSelfTradeProtection::NONE,
        side: BinanceSide::BUY,
        stopPrice: None,
        strategyId: None,
        strategyType: None,
        symbol: "BTCUSDT".into(),
        timeInForce: Some(BinanceTimeInForce::GTC),
        timestamp: 1700000000000,
        trailingDelta: None,
        r#type: BinanceOrderType::LIMIT,
    }
}

struct IgnoreHttpListener;

#[async_trait]
impl ListenerTrait for IgnoreHttpListener {
    type TMessage = BinanceHttpResponse;

    async fn on_message(&self, _message: BinanceHttpResponse) -> EGResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct MockHttpClient {
    sent: Arc<Mutex<Vec<HttpRequest>>>,
    server_time_offset: i64,
}

#[async_trait]
impl HttpClientTrait for MockHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        _endpoint: &str,
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        let is_time = matches!(message.params, BinanceHttpUnsignedRequest::Time(..));
        self.sent.lock().unwrap().push(message);
        Ok(HttpResponse {
            status: 200,
            body: br#"[]"#.to_vec(),
            headers: vec![],
        })
    }
}

/// Builds an HTTP connector backed by the scripted mock client, handing
/// the caller a handle to the client so sent requests can be inspected.
/// `server_time_offset` shifts the clock the mock's `time` responses
/// report, mirroring the production bootstrap (a server-time sync before
/// any signed request).
fn mock_http_connector(
    client_handle: std::sync::mpsc::Sender<MockHttpClient>,
    server_time_offset: i64,
) -> EGResult<impl Connector<BinanceHttpUnsignedRequest, BinanceHttpResponse>> {
    let credentials = ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    };
    let listener: Arc<dyn ListenerTrait<TMessage = BinanceHttpResponse>> =
        Arc::new(IgnoreHttpListener);
    let to_unsigned_request: ArcTryConvertValue<
        BinanceHttpUnsignedRequest,
        BinanceHttpUnsignedRequest,
    > = Arc::new(Ok);
    let to_external_response: ArcTryConvertValue<BinanceHttpResponse, BinanceHttpResponse> =
        Arc::new(Ok);
    let mock_client = MockHttpClient {
        sent: Arc::new(Mutex::new(Vec::new())),
        server_time_offset,
    };
    let _ = client_handle.send(mock_client.clone());
    let client: Arc<dyn HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse>> =
        Arc::new(mock_client);
    let clock = Arc::new(Clock::default());
    connector_with_client(
        client,
        clock.clone(),
        rate_limits(clock.clone()),
        to_unsigned_request,
        to_external_response,
        listener,
        Some(credentials),
    )
}

fn asset_limits_request() -> BinanceHttpUnsignedRequest {
    BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
        recvWindow: None,
        symbols: None,
        timestamp: 0,
    })
}

#[tokio::test]
async fn http_connector_installs_signer_on_connect() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = mock_http_connector(client_tx, 0).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(
        connector.is_authenticated().unwrap(),
        "connecting with credentials must install the request signer"
    );

    // The only request during connect is the unsigned time bootstrap.
    assert_eq!(client.sent.lock().unwrap().len(), 1);
    assert!(matches!(
        client.sent.lock().unwrap()[0].params,
        BinanceHttpUnsignedRequest::Time(..)
    ));
    assert!(
        client.sent.lock().unwrap()[0].signature.is_none(),
        "the time bootstrap must be unsigned"
    );

    // A signed request must not fail with NotAuthenticated.
    connector
        .send(asset_limits_request(), true, Duration::from_secs(5))
        .await
        .expect("signed send should succeed");
    assert_eq!(client.sent.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn http_connect_syncs_the_server_clock_before_signed_requests() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    // The server clock is 10 s ahead of the local clock: a signed request
    // stamped with the raw local clock would be rejected with -1021.
    let connector = mock_http_connector(client_tx, 10_000).unwrap();
    let client = client_rx.recv().unwrap();

    connector.connect().await.expect("connect should succeed");
    assert!(matches!(
        client.sent.lock().unwrap()[0].params,
        BinanceHttpUnsignedRequest::Time(..)
    ));

    // A signed request is stamped with the server clock, not the raw local
    // clock.
    connector
        .send(asset_limits_request(), true, Duration::from_secs(5))
        .await
        .expect("signed send should succeed");
    let sent = client.sent.lock().unwrap();
    let BinanceHttpUnsignedRequest::AssetLimits(asset_limits) = &sent[1].params else {
        panic!("expected an asset limits request");
    };
    let local = Clock::default().now_millis();
    assert!(
        asset_limits.timestamp >= local + 10_000,
        "timestamp {} must be near the server clock (local {local})",
        asset_limits.timestamp
    );
}

/// The outcome every request answered by a [`ScriptedHttpClient`] takes.
#[derive(Clone)]
enum ScriptedOutcome {
    /// A server-side 429/418 rejection (not counted against the budget).
    RateLimited,
    /// A 4xx/5xx business rejection, e.g. -2010 insufficient balance
    /// (counted against the budget).
    HttpError,
}

/// A scripted HTTP client: records every outgoing request and answers
/// with a fixed outcome, so send-failure budget behaviour can be tested
/// without a network.
#[derive(Clone)]
struct ScriptedHttpClient {
    sent: Arc<Mutex<Vec<HttpRequest>>>,
    outcome: ScriptedOutcome,
}

#[async_trait]
impl HttpClientTrait for ScriptedHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        _endpoint: &str,
        message: Self::TransportReq,
        _timeout: Duration,
    ) -> EGResult<Self::TransportRes> {
        self.sent.lock().unwrap().push(message);
        match self.outcome {
            ScriptedOutcome::RateLimited => Err(EGError::RateLimited(RateLimitFeedback {
                is_throttled: true,
                retry_after: Some(Duration::from_millis(50)),
                usage: vec![],
            })),
            ScriptedOutcome::HttpError => Err(EGError::HttpError {
                status: 400,
                body: br#"{"code":-2010,"msg":"insufficient balance"}"#.to_vec(),
            }),
        }
    }
}

/// Builds an HTTP connector backed by a scripted client answering with
/// `outcome`, using the given rate limits so the budget left after a
/// failed send can be observed.
fn scripted_http_connector(
    client_handle: std::sync::mpsc::Sender<ScriptedHttpClient>,
    outcome: ScriptedOutcome,
    rate_limits: RateLimits,
) -> EGResult<impl Connector<BinanceHttpUnsignedRequest, BinanceHttpResponse>> {
    let credentials = ApiKeyCredentials {
        api_key: "api-key".into(),
        secret: SecretString::from("secret"),
    };
    let listener: Arc<dyn ListenerTrait<TMessage = BinanceHttpResponse>> =
        Arc::new(IgnoreHttpListener);
    let to_unsigned_request: ArcTryConvertValue<
        BinanceHttpUnsignedRequest,
        BinanceHttpUnsignedRequest,
    > = Arc::new(Ok);
    let to_external_response: ArcTryConvertValue<BinanceHttpResponse, BinanceHttpResponse> =
        Arc::new(Ok);
    let scripted_client = ScriptedHttpClient {
        sent: Arc::new(Mutex::new(Vec::new())),
        outcome,
    };
    let _ = client_handle.send(scripted_client.clone());
    let client: Arc<dyn HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse>> =
        Arc::new(scripted_client);
    let clock = Arc::new(Clock::default());
    connector_with_client(
        client,
        clock,
        rate_limits,
        to_unsigned_request,
        to_external_response,
        listener,
        Some(credentials),
    )
}

fn single_slot_rate_limits() -> RateLimits {
    RateLimits {
        weight: RateLimiter::new(
            Arc::new(Clock::default()),
            vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }],
        ),
        orders: RateLimiter::new(
            Arc::new(Clock::default()),
            vec![RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            }],
        ),
    }
}

fn spot_order_request() -> BinanceHttpUnsignedRequest {
    BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()))
}

#[tokio::test]
async fn http_send_keeps_local_reservation_on_business_rejection() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = scripted_http_connector(
        client_tx,
        ScriptedOutcome::HttpError,
        single_slot_rate_limits(),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    // The order is rejected with a 4xx business error (-2010 etc.), but
    // Binance counts its weight anyway: the locally-reserved capacity
    // must not be refunded.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(
        result,
        Err(EGError::HttpError { status: 400, .. })
    ));

    // The budget stays exhausted, so the next send is rejected locally
    // and never reaches the transport.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_send_refunds_local_reservation_on_rate_limited() {
    let (client_tx, client_rx) = std::sync::mpsc::channel();
    let connector = scripted_http_connector(
        client_tx,
        ScriptedOutcome::RateLimited,
        single_slot_rate_limits(),
    )
    .unwrap();
    let client = client_rx.recv().unwrap();

    // A server-side 429 is not counted against the request-weight budget,
    // so the locally-reserved capacity is refunded.
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 1);

    // Once the server's Retry-After has elapsed, the refunded budget
    // admits the next request: it reaches the transport again instead of
    // being rejected by the local limiter.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = connector
        .send(spot_order_request(), false, Duration::from_secs(5))
        .await;
    assert!(matches!(result, Err(EGError::RateLimited { .. })));
    assert_eq!(client.sent.lock().unwrap().len(), 2);
}
