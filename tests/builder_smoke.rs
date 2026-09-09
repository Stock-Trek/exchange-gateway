use exchange_gateway::{
    clients::{client::WebsocketClient, reqwest::ReqwestHttpClient},
    connector::{Connector, ConnectorBuilder},
    error::{EGError, EGResult},
    functions::ArcTryConvertValue,
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
};
use exchange_types::{
    encode::ByteEncoder,
    encrypt::Encryptor,
    new_types::UsageCount,
    rate_limited::{RateLimit, RateLimitRestriction, RateLimits},
    signer::Signer,
    urls::{Protocol, TradingMode, Urls},
};
use std::{collections::HashMap, sync::Arc, time::Duration};

struct TestUrls;

impl Urls for TestUrls {
    fn name(&self) -> &'static str {
        "test"
    }
    fn url(&self, protocol: Protocol, _trading_mode: TradingMode) -> &str {
        match protocol {
            Protocol::Http => "http://example.com",
            Protocol::Websocket => "ws://example.com",
        }
    }
}

struct TestRateLimits;

impl RateLimits for TestRateLimits {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        let mut map = HashMap::new();
        map.insert(
            RateLimit {
                restriction: RateLimitRestriction::RawRequests,
                interval_nanos: exchange_types::new_types::Nanoseconds(1_000_000_000),
            },
            UsageCount(100),
        );
        map
    }
}

struct TestListener;

#[async_trait::async_trait]
impl ListenerTrait for TestListener {
    type TMessage = serde_json::Value;

    async fn on_message(&self, _message: serde_json::Value) -> EGResult<()> {
        Ok(())
    }
}

struct TestWebsocketClient;

#[async_trait::async_trait]
impl WebsocketClient for TestWebsocketClient {
    async fn connect(&self) -> EGResult<()> {
        Ok(())
    }
    fn is_connected(&self) -> bool {
        false
    }
    async fn send(&self, _message: String, _timeout: Duration) -> EGResult<()> {
        Ok(())
    }
    async fn disconnect(&self) -> EGResult<()> {
        Ok(())
    }
}

fn signer() -> Signer {
    Signer::new(
        "key".into(),
        Encryptor::HmacSha256(secrecy::SecretSlice::from(b"secret".to_vec())),
        ByteEncoder::HexLower,
    )
}

#[test]
fn builder_defaults_build_http_reqwest() {
    let connector: Connector<ReqwestHttpClient> =
        ConnectorBuilder::new().build_http_reqwest().unwrap();
    assert!(connector.server_time_millis().is_ok());
}

#[test]
fn builder_with_all_fields_build_http_custom_client() {
    let connector = ConnectorBuilder::new()
        .trading_mode(TradingMode::Real)
        .urls(TestUrls)
        .rate_limits(TestRateLimits)
        .signer(signer())
        .build_http(Box::new(|url: String| {
            assert_eq!(url, "http://example.com");
            Ok(ReqwestHttpClient::new(&url))
        }))
        .unwrap();
    assert!(connector.server_time_millis().is_ok());
}

#[test]
fn builder_defaults_build_websocket_custom_client() {
    let creator: exchange_gateway::functions::BoxTryCreateOnce<
        (String, Arc<WebsocketListener<serde_json::Value, serde_json::Value>>),
        TestWebsocketClient,
    > = Box::new(|(url, _)| {
        assert_eq!(url, "ws://localhost");
        Ok(TestWebsocketClient)
    });
    let connector = ConnectorBuilder::new().build_websocket(creator).unwrap();
    assert!(!connector.is_connected().unwrap());
}

#[test]
fn builder_with_converter_and_listener_build_websocket() {
    let converter: ArcTryConvertValue<String, serde_json::Value> = Arc::new(|value: String| {
        serde_json::from_str(&value).map_err(|e| EGError::External(Box::new(e)))
    });
    let creator: exchange_gateway::functions::BoxTryCreateOnce<
        (String, Arc<WebsocketListener<String, serde_json::Value>>),
        TestWebsocketClient,
    > = Box::new(|(url, _)| {
        assert_eq!(url, "ws://example.com");
        Ok(TestWebsocketClient)
    });
    let connector = ConnectorBuilder::new()
        .trading_mode(TradingMode::Paper)
        .urls(TestUrls)
        .rate_limits(TestRateLimits)
        .signer(signer())
        .listener(TestListener)
        .converter(converter)
        .build_websocket(creator)
        .unwrap();
    assert!(!connector.is_connected().unwrap());
}

#[cfg(feature = "iris")]
#[test]
fn builder_defaults_build_websocket_iris() {
    let connector = ConnectorBuilder::new()
        .urls(TestUrls)
        .build_websocket_iris(iris::Config::default())
        .unwrap();
    assert!(!connector.is_connected().unwrap());
}
