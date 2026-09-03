use exchange_gateway::{
    async_trait,
    listeners::websocket_listener::WebsocketListener,
    prelude::*,
    transports::http::{HttpClientTrait, HttpRequest, HttpResponse},
    transports::websocket::WebsocketClientTrait,
};
use exchange_types::{
    binance::{
        http::{
            BinanceHttpRequest, BinanceHttpResponsePayload, BinanceHttpResponseResult,
            BinanceHttpUnsignedRequest,
        },
        time::{BinanceTimeParams, BinanceTimeResult},
        websocket::{
            BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketResponseResult,
            BinanceWebsocketSignedParams, BinanceWebsocketUnsignedParams,
        },
    },
    http::HttpMethod,
    urls::TradingMode,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct MockHttpClient {
    requests: Arc<Mutex<Vec<HttpRequest>>>,
    server_time: i64,
}

#[async_trait]
impl HttpClientTrait for MockHttpClient {
    type TransportReq = HttpRequest;
    type TransportRes = HttpResponse;

    async fn send_message(
        &self,
        message: HttpRequest,
        _timeout: Duration,
    ) -> EGResult<HttpResponse> {
        self.requests.lock().unwrap().push(message);
        Ok(HttpResponse {
            status: 200,
            headers: vec![],
            body: format!(r#"{{"serverTime":{}}}"#, self.server_time).into_bytes(),
        })
    }
}

#[tokio::test]
async fn binance_http_accepts_a_caller_provided_client() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mock_requests = requests.clone();
    let connector = Connect::binance_http(
        TradingMode::Paper,
        Clock::default(),
        Box::new(move |_url| {
            Ok(MockHttpClient {
                requests: mock_requests,
                server_time: 1_700_000_000_000,
            })
        }),
    )
    .expect("connector construction should succeed");
    connector.connect().await.expect("connect should succeed");
    assert!(matches!(connector.is_connected(), Ok(true)));
    let response = connector
        .send(
            BinanceHttpRequest {
                unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
                signature: None,
            },
            Duration::from_secs(5),
        )
        .await
        .expect("send should succeed");
    connector
        .disconnect()
        .await
        .expect("disconnect should succeed");
    let payload = match response.payload {
        BinanceHttpResponsePayload::Success(result) => result,
        failure => panic!("expected a success payload, got: {failure:?}"),
    };
    let time = match payload {
        BinanceHttpResponseResult::Time(time) => time,
        other => panic!("expected a time result, got: {other:?}"),
    };
    assert_eq!(time.serverTime, 1_700_000_000_000);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(matches!(requests[0].method, HttpMethod::GET));
    assert_eq!(requests[0].query.as_deref(), Some("time?"));
}

struct NoopListener;

#[async_trait]
impl ListenerTrait for NoopListener {
    type TMessage = BinanceWebsocketResponse;

    async fn on_message(&self, _message: BinanceWebsocketResponse) -> EGResult<()> {
        Ok(())
    }
}

struct MockWebsocketClient {
    connected: Arc<AtomicBool>,
    listener: Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
}

#[async_trait]
impl WebsocketClientTrait for MockWebsocketClient {
    type TransportReq = BinanceWebsocketRequest;
    type TransportRes = BinanceWebsocketResponse;

    async fn connect(&self) -> EGResult<()> {
        self.connected.store(true, Ordering::SeqCst);
        self.listener.on_connected().await
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    async fn send_message(
        &self,
        message: BinanceWebsocketRequest,
        _timeout: Duration,
    ) -> EGResult<()> {
        self.listener
            .on_message(BinanceWebsocketResponse {
                error: None,
                id: message.id.clone(),
                rateLimits: vec![],
                result: Some(BinanceWebsocketResponseResult::Time(BinanceTimeResult {
                    serverTime: 1_700_000_000_000,
                })),
                status: 200,
            })
            .await
    }

    async fn disconnect(&self) -> EGResult<()> {
        self.connected.store(false, Ordering::SeqCst);
        self.listener.on_disconnected().await
    }
}

#[tokio::test]
async fn binance_websocket_accepts_a_caller_provided_client() {
    let connected = Arc::new(AtomicBool::new(false));
    let connector = Connect::binance_websocket(
        TradingMode::Paper,
        Clock::default(),
        NoopListener,
        Box::new(
            move |(url, websocket_listener): (
                String,
                Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
            )| {
                assert!(!url.is_empty());
                Ok(MockWebsocketClient {
                    connected: connected.clone(),
                    listener: websocket_listener,
                })
            },
        ),
    )
    .expect("connector construction should succeed");
    connector.connect().await.expect("connect should succeed");
    assert!(matches!(connector.is_connected(), Ok(true)));
    let response = connector
        .send(
            BinanceWebsocketRequest {
                id: "time".into(),
                params: BinanceWebsocketSignedParams {
                    unsigned: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
                    signature: None,
                },
            },
            Duration::from_secs(5),
        )
        .await
        .expect("send should succeed");
    connector
        .disconnect()
        .await
        .expect("disconnect should succeed");
    match response.result {
        Some(BinanceWebsocketResponseResult::Time(time)) => {
            assert_eq!(time.serverTime, 1_700_000_000_000);
        }
        other => panic!("expected a time result, got: {other:?}"),
    }
}
