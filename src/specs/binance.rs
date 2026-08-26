use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{ArcCombineValues, ArcTryConvertValue},
    listeners::{
        convert_listener::ConvertListener, listener::ListenerTrait,
        websocket_listener::WebsocketListener,
    },
    rate_limit::{
        rate_limit_config::RateLimitConfig, rate_limiter::RateLimiter, rate_limits::RateLimits,
    },
    sign::{
        convert_signer::ConvertSigner,
        encode::byte_encoding::ByteEncoding,
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
        message_signer::MessageSigner,
        signer::Signer,
    },
    transports::{
        http::{HttpClientTrait, HttpEndpoint, HttpTransport},
        transport::Transport,
        websocket::{WebsocketClientTrait, WebsocketTransport},
    },
    urls::{ExchangeTransportType, ExchangeTransportUrls, ExchangeUrls, TradingMode},
};
use exchange_types::binance::{
    http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    logon::BinanceLogonParams,
    signed::{BinanceSignature, BinanceSignedParams},
    spot::BinanceSpotOrderParams,
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketUnsignedParams, BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[allow(clippy::too_many_arguments)]
pub fn http_connector<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes>(
    trading_mode: TradingMode,
    create_client: impl Fn(&str) -> TClient,
    to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
    to_transport_request: ArcTryConvertValue<BinanceHttpRequest, HttpReq>,
    to_binance_response: ArcTryConvertValue<HttpRes, BinanceHttpResponse>,
    to_external_response: ArcTryConvertValue<BinanceHttpResponse, ExternalRes>,
    listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
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
    let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceHttpResponse>> =
        Arc::new(ConvertListener::new(to_external_response, listener));
    let http_transport = HttpTransport::new(
        client,
        to_transport_request,
        to_binance_response,
        response_listener,
        request_to_http_endpoint,
        http_endpoints(),
    );
    let signer = Arc::new(Mutex::new(credentials.as_ref().map(|credentials| {
        create_http_signer_from_credentials(credentials)
            .expect("Failed to create signer from credentials")
    })));
    ConnectorImpl {
        rate_limits: rate_limits(),
        to_weight: http_request_weight,
        to_unsigned_request,
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
    to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    to_transport_request: ArcTryConvertValue<BinanceWebsocketRequest, WebsocketReq>,
    to_binance_response: ArcTryConvertValue<WebsocketRes, BinanceWebsocketResponse>,
    to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
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
    let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
        Arc::new(ConvertListener::new(to_external_response, listener));
    let websocket_listener = Arc::new(WebsocketListener::new(
        to_binance_response.clone(),
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
        to_weight: websocket_request_weight,
        to_unsigned_request,
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
            "https://api.binance.com/api/v3",
            "https://testnet.binance.vision/api/v3",
        ),
        ExchangeTransportUrls::new(
            "wss://ws-api.binance.com:443/ws-api/v3",
            "wss://ws-api.testnet.binance.vision:443/ws-api/v3",
        ),
    )
}
fn request_to_http_endpoint(request: &BinanceHttpRequest) -> HttpEndpoint {
    match request.params {
        BinanceHttpUnsignedRequest::AssetLimits => HttpEndpoint::AssetLimits,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => HttpEndpoint::ExchangeInfo,
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => HttpEndpoint::PlaceOrder,
    }
}
fn http_endpoints() -> HashMap<HttpEndpoint, String> {
    let mut endpoints = HashMap::new();
    endpoints.insert(HttpEndpoint::AssetLimits, "myFilters".into());
    endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
    endpoints.insert(HttpEndpoint::PlaceOrder, "order".into());
    endpoints
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
        Arc::new(move |response: &BinanceWebsocketResponse| {
            response.id == *id && response.error.is_none() && response.status == 200
        })
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
        Arc::new(http_unsigned_request_to_bytes),
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender_http(api_key.into()),
    )))
}
fn create_websocket_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    let to_bytes = {
        let api_key = api_key.clone();
        Arc::new(move |request: &BinanceWebsocketUnsignedRequest| {
            websocket_unsigned_request_params_to_bytes(&api_key, request)
        })
    };
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        to_bytes,
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
    api_key: &str,
    request: &BinanceWebsocketUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match &request.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
            Some(signed_payload(api_key, &params.query_params(true)))
        }
        BinanceWebsocketUnsignedParams::Logon(params) => {
            Some(signed_payload(api_key, &params.query_params(true)))
        }
        BinanceWebsocketUnsignedParams::ExchangeInfo(_) => None,
    })
}
/// Binance's documented ws-api signing rule: the signature is the HMAC-SHA256 of
/// all request params except `signature`, sorted by key and joined with `&`,
/// **including `apiKey`** (unlike REST, where `apiKey` travels in a header).
///
/// See <https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api/signed-request-security>
/// for the canonical example: `apiKey=...&price=...&...&type=LIMIT`.
fn signed_payload(api_key: &str, query: &str) -> Vec<u8> {
    if query.is_empty() {
        format!("apiKey={api_key}").into_bytes()
    } else {
        format!("apiKey={api_key}&{query}").into_bytes()
    }
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

fn rate_limits() -> RateLimits {
    RateLimits {
        request: RateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 1200,
            interval_nanos: Duration::from_mins(1).as_nanos(),
        }]),
    }
}

fn http_request_weight(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::AssetLimits => 1,
        BinanceHttpUnsignedRequest::ExchangeInfo(_) => 20,
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => order_weight(params),
    }
}
fn websocket_request_weight(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        // `session.logon` is documented as weight 2; `exchangeInfo` as weight 20
        // on the ws-api (previously both were counted as 1).
        BinanceWebsocketUnsignedParams::Logon(_) => 2,
        BinanceWebsocketUnsignedParams::ExchangeInfo(_) => 20,
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => order_weight(params),
    }
}
fn order_weight(params: &BinanceSpotOrderParams) -> u32 {
    if params.icebergQty.is_some()
        || params.trailingDelta.is_some()
        || params.pegPriceType.is_some()
        || params.pegOffsetValue.is_some()
    {
        2
    } else {
        1
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

#[cfg(test)]
mod tests {
    use super::*;
    use exchange_types::binance::error::BinanceError;

    fn logon_response(
        id: String,
        status: i32,
        error: Option<BinanceError>,
    ) -> BinanceWebsocketResponse {
        BinanceWebsocketResponse {
            error,
            id,
            rateLimits: vec![],
            result: None,
            status,
        }
    }

    #[test]
    fn websocket_logon_signature_matches_binances_documented_example() {
        // Values from Binance's official ws-api "Log in with API key" example:
        // https://developers.binance.com/docs/binance-spot-api-docs/web-socket-api/signed-request-security
        // The signature must be computed over `apiKey` AND `timestamp` (sorted),
        // not over the params alone.
        let credentials = ApiKeyCredentials {
            api_key: "vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A".into(),
            secret: SecretString::from(
                "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
            ),
        };
        let signer = create_websocket_signer_from_credentials(&credentials).unwrap();
        let request = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "c174a2b1-3f51-4580-b200-8528bd237cb7".into(),
                method: BinanceWebsocketMethodName::Logon,
            },
            params: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams {
                timestamp: 1649729878532,
            }),
        };
        let BinanceWebsocketRequest { params, .. } = signer.sign(request).unwrap();
        let signature = params.signature.expect("logon must be signed");
        assert_eq!(signature.apiKey, credentials.api_key);
        assert_eq!(
            signature.signature,
            "1cf54395b336b0a9727ef27d5d98987962bc47aca6e13fe978612d0adee066ed"
        );
    }

    #[test]
    fn signed_payload_prepends_api_key() {
        assert_eq!(
            signed_payload("KEY", "timestamp=123"),
            b"apiKey=KEY&timestamp=123"
        );
        assert_eq!(signed_payload("KEY", ""), b"apiKey=KEY");
    }

    #[test]
    fn logon_filter_only_matches_successful_logon_response() {
        let leg = authenticate_websocket_leg();
        let id = (leg.create_auth_message)().metadata.id;
        assert!((leg.filter)(&logon_response(id.clone(), 200, None)));
        assert!(!(leg.filter)(&logon_response(
            id.clone(),
            200,
            Some(BinanceError {
                code: -2014,
                msg: "API-key format invalid.".into(),
            }),
        )));
        assert!(!(leg.filter)(&logon_response(id.clone(), 401, None)));
        assert!(!(leg.filter)(&logon_response(
            "some-other-id".into(),
            200,
            None
        )));
    }
}
