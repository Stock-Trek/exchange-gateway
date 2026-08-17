use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{ArcCombineValues, TryConvertValue},
    listeners::{
        convert_listener::ConvertListener, listener::ListenerTrait,
        websocket_listener::WebsocketListener,
    },
    rate_limit::{
        rate_limit_config::RateLimitConfig, rate_limiter::RateLimiter, rate_limits::RateLimits,
        request_weights::RequestWeights,
    },
    sign::{
        convert_signer::ConvertSigner,
        encode::byte_encoding::ByteEncoding,
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
        message_signer::MessageSigner,
        signer::Signer,
    },
    transports::{
        http::{HttpClientTrait, HttpTransport},
        transport::Transport,
        websocket::{WebsocketClientTrait, WebsocketTransport},
    },
    urls::{ExchangeTransportType, ExchangeTransportUrls, ExchangeUrls, TradingMode},
};
use exchange_types::binance::{
    http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    logon::BinanceLogonParams,
    signed::{BinanceSignature, BinanceSignedParams},
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketUnsignedParams, BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[allow(clippy::too_many_arguments)]
pub fn http_connector<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes>(
    trading_mode: TradingMode,
    create_client: impl Fn(&str) -> TClient,
    to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
    to_transport_request: TryConvertValue<BinanceHttpRequest, HttpReq>,
    to_binance_response: TryConvertValue<HttpRes, BinanceHttpResponse>,
    to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
    credentials: Option<ApiKeyCredentials>,
) -> impl Connector<ExternalReq, ExternalRes>
where
    TClient: HttpClientTrait<TransportReq = HttpReq, TransportRes = HttpRes> + 'static,
    ExternalReq: Send,
    HttpReq: Send,
    HttpRes: Send + 'static,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let exchange_urls = exchange_urls();
    let url = exchange_urls.url(ExchangeTransportType::Http, trading_mode);
    let client = Arc::new((create_client)(&url));
    let http_transport = HttpTransport::new(client, to_transport_request, to_binance_response);
    let signer = Arc::new(Mutex::new(credentials.as_ref().map(|credentials| {
        create_http_signer_from_credentials(credentials)
            .expect("Failed to create signer from credentials")
    })));
    ConnectorImpl {
        rate_limits: rate_limits(),
        request_weights: request_weights(),
        to_unsigned_request,
        to_external_response: Arc::new(to_external_response),
        transport: Transport::Http(http_transport),
        null_signer: null_http_signer(),
        credentials,
        create_signer: create_http_signer_from_credentials,
        authenticate_legs: vec![],
        signer,
    }
}
#[allow(clippy::too_many_arguments)]
pub fn websocket_connector<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes>(
    trading_mode: TradingMode,
    create_client: impl Fn(&str, Arc<dyn ListenerTrait<TMessage = WebsocketRes>>) -> TClient,
    to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    to_transport_request: TryConvertValue<BinanceWebsocketRequest, WebsocketReq>,
    to_binance_response: TryConvertValue<WebsocketRes, BinanceWebsocketResponse>,
    to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
    listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    credentials: Option<ApiKeyCredentials>,
    use_session: bool,
) -> impl Connector<ExternalReq, ExternalRes>
where
    TClient:
        WebsocketClientTrait<TransportReq = WebsocketReq, TransportRes = WebsocketRes> + 'static,
    ExternalReq: Send,
    WebsocketReq: Send,
    WebsocketRes: Send + 'static,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let exchange_urls = exchange_urls();
    let url = exchange_urls.url(ExchangeTransportType::Websocket, trading_mode);
    let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> = Arc::new(
        ConvertListener::new(Arc::new(to_external_response), listener),
    );
    let websocket_listener = Arc::new(WebsocketListener::new(
        to_binance_response,
        response_listener,
    ));
    let client = Arc::new((create_client)(&url, websocket_listener.clone()));
    let websocket_transport = WebsocketTransport::new(
        client,
        to_transport_request,
        to_binance_response,
        websocket_listener,
    );
    let (authenticate_legs, signer) = if use_session {
        (
            vec![authenticate_websocket_leg()],
            Arc::new(Mutex::new(None)),
        )
    } else {
        (
            vec![],
            Arc::new(Mutex::new(credentials.as_ref().map(|credentials| {
                create_websocket_signer_from_credentials(credentials)
                    .expect("Failed to create signer from credentials")
            }))),
        )
    };
    ConnectorImpl {
        rate_limits: rate_limits(),
        request_weights: request_weights(),
        to_unsigned_request,
        to_external_response: Arc::new(to_external_response),
        transport: Transport::Websocket(websocket_transport),
        null_signer: null_websocket_signer(),
        credentials,
        create_signer: create_websocket_signer_from_credentials,
        authenticate_legs,
        signer,
    }
}

fn exchange_urls() -> ExchangeUrls {
    ExchangeUrls::new(
        "BINANCE",
        ExchangeTransportUrls::new(
            "https://api.binance.com",
            "https://testnet.binance.vision/api",
        ),
        ExchangeTransportUrls::new(
            "wss://ws-fapi.binance.com/ws-fapi/v1",
            "wss://testnet.binancefuture.com/ws-fapi/v1",
        ),
    )
}

fn authenticate_websocket_leg() -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let timeout = Duration::from_secs(20);
    let id = Arc::new(id());
    let create_auth_message = {
        let id = id.clone();
        Arc::new(move || create_auth_message(&id))
    };
    let filter = {
        let id = id.clone();
        Arc::new(move |response: &BinanceWebsocketResponse| response.id == *id)
    };
    AuthenticateLeg {
        create_auth_message,
        create_signer: create_signer_from_message,
        filter,
        timeout,
    }
}
fn create_auth_message(id: &str) -> BinanceWebsocketUnsignedRequest {
    let timestamp: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Negative time since epoch")
        .as_millis()
        .try_into()
        .expect("Epoch too large");
    let params = BinanceLogonParams { timestamp };
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id: id.to_string(),
            method: BinanceWebsocketMethodName::Logon,
        },
        params: BinanceWebsocketUnsignedParams::Logon(params),
    }
}
fn create_signer_from_message(
    _message: BinanceWebsocketResponse,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    Ok(Box::new(ConvertSigner::new(websocket_converter)))
}

fn null_http_signer() -> ConvertSigner<BinanceHttpUnsignedRequest, BinanceHttpRequest> {
    ConvertSigner::new(|unsigned| {
        Ok(BinanceHttpRequest {
            params: unsigned,
            signature: None,
        })
    })
}
fn null_websocket_signer() -> ConvertSigner<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>
{
    ConvertSigner::new(|unsigned| {
        let BinanceWebsocketUnsignedRequest { metadata, params } = unsigned;
        Ok(BinanceWebsocketRequest {
            metadata,
            params: BinanceSignedParams {
                params,
                signature: None,
            },
        })
    })
}

fn create_http_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceHttpUnsignedRequest, BinanceHttpRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceHttpUnsignedRequest,
        BinanceHttpRequest,
    >::new(
        http_unsigned_request_to_bytes,
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender_http(api_key.into()),
    )))
}
fn create_websocket_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        websocket_unsigned_request_params_to_bytes,
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender_websocket(api_key.into()),
    )))
}
fn http_unsigned_request_to_bytes(
    request: &BinanceHttpUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match request {
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        _ => None,
    })
}
fn websocket_unsigned_request_params_to_bytes(
    request: &BinanceWebsocketUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match &request.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::Logon(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::ExchangeInfo(_) => None,
    })
}
fn data_signer(secret: &SecretString) -> EGResult<DataSigner> {
    SigningAlgorithm::HmacSha256.signer(secret)
}

fn websocket_converter(
    unsigned: BinanceWebsocketUnsignedRequest,
) -> EGResult<BinanceWebsocketRequest> {
    let BinanceWebsocketUnsignedRequest { metadata, params } = unsigned;
    let params = BinanceSignedParams {
        signature: None,
        params,
    };
    Ok(BinanceWebsocketRequest { metadata, params })
}

// TODO ensure this is correct
fn rate_limits() -> RateLimits {
    RateLimits {
        send_order_request: RateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 1200,
            interval_nanos: Duration::from_mins(1).as_nanos(),
        }]),
    }
}

// TODO ensure this is correct
fn request_weights() -> RequestWeights {
    RequestWeights {
        send_order_request: 1,
    }
}

fn signature_appender_http(
    api_key: String,
) -> ArcCombineValues<BinanceHttpUnsignedRequest, Option<String>, BinanceHttpRequest> {
    Arc::new(move |unsigned, signature| {
        let signature = signature.map(|signature| BinanceSignature {
            apiKey: api_key.to_string(),
            signature,
        });
        BinanceHttpRequest {
            params: unsigned,
            signature,
        }
    })
}
fn signature_appender_websocket(
    api_key: String,
) -> ArcCombineValues<BinanceWebsocketUnsignedRequest, Option<String>, BinanceWebsocketRequest> {
    Arc::new(move |unsigned, signature| {
        let BinanceWebsocketUnsignedRequest {
            metadata,
            params: unsigned_params,
        } = unsigned;
        let signature = signature.map(|signature| BinanceSignature {
            apiKey: api_key.to_string(),
            signature,
        });
        let params = BinanceSignedParams {
            params: unsigned_params,
            signature,
        };
        BinanceWebsocketRequest { metadata, params }
    })
}

fn id() -> String {
    Uuid::new_v4().to_string()
}
