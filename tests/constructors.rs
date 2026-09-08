//! Regression test for #293: the `Connector` constructors must be callable as
//! `Connector::try_new_*` without turbofish annotations on the `Client` type
//! parameter (the type is determined by what the constructor returns).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use exchange_gateway::async_trait;
use exchange_gateway::clients::client::{HttpClient, WebsocketClient};
use exchange_gateway::connector::Connector;
use exchange_gateway::error::EGResult;
use exchange_gateway::listeners::listener::ListenerTrait;
use exchange_gateway::listeners::websocket_listener::WebsocketListener;
use exchange_types::api_key_credential::ApiKeyCredentials;
use exchange_types::encode::ByteEncoder;
use exchange_types::encrypt::EncryptionAlgorithm;
use exchange_types::http::{HttpRequest, HttpResponse};
use exchange_types::new_types::UsageCount;
use exchange_types::rate_limited::{RateLimit, RateLimits};
use exchange_types::signer::Signer;
use exchange_types::urls::{Protocol, TradingMode, Urls};
use secrecy::SecretString;
use serde_json::Value;

struct TestUrls;

impl Urls for TestUrls {
    fn name(&self) -> &'static str {
        "test"
    }
    fn url(&self, _protocol: Protocol, _trading_mode: TradingMode) -> &str {
        "example.com"
    }
}

struct TestRateLimits;

impl RateLimits for TestRateLimits {
    fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
        HashMap::new()
    }
}

fn signer() -> Signer {
    let encryptor = EncryptionAlgorithm::HmacSha256
        .encryptor(ApiKeyCredentials {
            api_key: "api-key".to_string(),
            secret: SecretString::from("api-secret"),
        })
        .expect("signer");
    Signer::new("api-key".to_string(), encryptor, ByteEncoder::Base64)
}

struct TestListener;

#[async_trait]
impl ListenerTrait for TestListener {
    type TMessage = Value;

    async fn on_message(&self, _message: Value) -> EGResult<()> {
        Ok(())
    }
}

struct TestClient;

#[async_trait]
impl HttpClient for TestClient {
    async fn send(&self, _request: HttpRequest, _timeout: Duration) -> EGResult<HttpResponse> {
        unreachable!("construction tests never send")
    }
}

#[async_trait]
impl WebsocketClient for TestClient {
    async fn connect(&self) -> EGResult<()> {
        Ok(())
    }
    fn is_connected(&self) -> bool {
        true
    }
    async fn send(&self, _message: String, _timeout: Duration) -> EGResult<()> {
        Ok(())
    }
    async fn disconnect(&self) -> EGResult<()> {
        Ok(())
    }
}

#[test]
fn http_constructor_infers_client_from_creator() {
    let connector = Connector::try_new_http(
        TradingMode::Real,
        &TestUrls,
        TestRateLimits,
        signer(),
        Box::new(move |_url: String| Ok(TestClient)),
    )
    .expect("constructor");
    let _ = connector.server_time_millis();
}

#[test]
#[cfg(feature = "reqwest")]
fn http_reqwest_constructor_needs_no_turbofish() {
    let connector =
        Connector::try_new_http_reqwest(TradingMode::Real, &TestUrls, TestRateLimits, signer())
            .expect("constructor");
    let _ = connector.server_time_millis();
}

#[test]
#[cfg(feature = "reqwest")]
fn http_reqwest_constructor_is_a_usable_http_connector() {
    let connector =
        Connector::try_new_http_reqwest(TradingMode::Real, &TestUrls, TestRateLimits, signer())
            .expect("constructor");
    let _: Connector<exchange_gateway::clients::reqwest::ReqwestHttpClient> = connector;
}

#[test]
fn websocket_constructor_infers_client_and_transport() {
    let converter: exchange_gateway::functions::ArcTryConvertValue<Value, Value> =
        Arc::new(|value: Value| -> EGResult<Value> { Ok(value) });
    let connector = Connector::try_new_websocket(
        TradingMode::Real,
        &TestUrls,
        TestRateLimits,
        signer(),
        converter,
        TestListener,
        Box::new(
            move |(_url, _listener): (
                String,
                Arc<WebsocketListener<Value, Value>>,
            )| -> EGResult<TestClient> { Ok(TestClient) },
        ),
    )
    .expect("constructor");
    let _ = connector.is_connected().expect("is_connected");
}

#[test]
#[cfg(feature = "iris")]
fn websocket_iris_constructor_needs_no_turbofish() {
    let connector = Connector::try_new_websocket_iris(
        TradingMode::Real,
        &TestUrls,
        TestRateLimits,
        signer(),
        TestListener,
        exchange_gateway::iris::Config::default(),
    )
    .expect("constructor");
    let _ = connector.is_connected().expect("is_connected");
}
