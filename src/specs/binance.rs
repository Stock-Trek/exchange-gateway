use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
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
        BinanceWebsocketResponse, BinanceWebsocketResponseResult, BinanceWebsocketUnsignedParams,
        BinanceWebsocketUnsignedRequest,
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
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    TClient:
        WebsocketClientTrait<TransportReq = WebsocketReq, TransportRes = WebsocketRes> + 'static,
    ExternalReq: Send,
    WebsocketReq: Send,
    WebsocketRes: Send + 'static,
    ExternalRes: Clone + Send + Sync + 'static,
{
    validate_use_session(&credentials, use_session)?;
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
    let authenticate_legs = if use_session {
        vec![authenticate_websocket_leg()]
    } else {
        vec![]
    };
    Ok(ConnectorImpl {
        rate_limits: rate_limits(),
        to_weight: websocket_request_weight,
        to_unsigned_request,
        transport: Transport::Websocket(websocket_transport),
        null_signer: null_websocket_signer(),
        credentials,
        create_signer: create_websocket_signer_from_credentials,
        authenticate_legs,
        signer: Arc::new(Mutex::new(None)),
    })
}

fn validate_use_session(
    credentials: &Option<ApiKeyCredentials>,
    use_session: bool,
) -> EGResult<()> {
    if !use_session && credentials.is_some() {
        return Err(EGError::InvalidConfiguration(
            "Binance ws-api requires session-based authentication: set use_session=true when \
             providing credentials for a websocket connector"
                .to_string(),
        ));
    }
    Ok(())
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
    message: BinanceWebsocketResponse,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    if let Some(error) = message.error {
        return Err(EGError::AuthenticationFailed(format!(
            "Binance ws-api session.logon failed: {} ({})",
            error.msg, error.code
        )));
    }
    if message.status != 200 {
        return Err(EGError::AuthenticationFailed(format!(
            "Binance ws-api session.logon failed with status {}",
            message.status
        )));
    }
    let session = match message.result {
        Some(BinanceWebsocketResponseResult::SessionAuthentication(session)) => session,
        _ => return Err(EGError::BadResponse),
    };
    let api_key = session.apiKey;
    let session_key = SecretString::from(session.sessionKey);
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        websocket_unsigned_request_params_to_bytes,
        data_signer(&session_key)?,
        ByteEncoding::HexLower,
        signature_appender_websocket(api_key),
    )))
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
        BinanceHttpUnsignedRequest::ExchangeInfo(_) | BinanceHttpUnsignedRequest::AssetLimits => 1,
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => order_weight(params),
    }
}
fn websocket_request_weight(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        BinanceWebsocketUnsignedParams::ExchangeInfo(_)
        | BinanceWebsocketUnsignedParams::Logon(_) => 1,
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
    use crate::sign::encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding};
    use exchange_types::binance::{
        error::BinanceError,
        exchange_info::BinanceOrderType,
        logon::BinanceSessionAuthenticationResult,
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams,
        },
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    fn logon_response(status: i32, api_key: &str, session_key: &str) -> BinanceWebsocketResponse {
        BinanceWebsocketResponse {
            error: None,
            id: "logon-1".into(),
            rateLimits: vec![],
            result: Some(BinanceWebsocketResponseResult::SessionAuthentication(
                BinanceSessionAuthenticationResult {
                    apiKey: api_key.into(),
                    authorizedSince: 0,
                    connectedSince: 0,
                    returnRateLimits: false,
                    serverTime: 0,
                    userDataStream: false,
                    sessionKey: session_key.into(),
                },
            )),
            status,
        }
    }

    fn unsigned_spot_order_request() -> BinanceWebsocketUnsignedRequest {
        let params = BinanceSpotOrderParams {
            icebergQty: None,
            newClientOrderId: "client-order-id".into(),
            newOrderRespType: BinanceNewOrderResponseType::ACK,
            pegPriceType: None,
            pegOffsetValue: None,
            pegOffsetType: None,
            price: None,
            quantity: None,
            quoteOrderQty: None,
            recvWindow: None,
            selfTradePreventionMode: BinanceSelfTradeProtection::NONE,
            side: BinanceSide::BUY,
            stopPrice: None,
            strategyId: None,
            strategyType: None,
            symbol: "BTCUSDT".into(),
            timeInForce: None,
            timestamp: 1_700_000_000_000,
            trailingDelta: None,
            r#type: BinanceOrderType::LIMIT,
        };
        BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "order-1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(params)),
        }
    }

    #[test]
    fn signer_from_logon_response_uses_session_key_as_hmac_secret() {
        let api_key = "api-key";
        let session_key = "session-secret";
        let response = logon_response(200, api_key, session_key);
        let signer =
            create_signer_from_message(response).expect("logon response should yield a signer");
        let unsigned = unsigned_spot_order_request();
        let signed = signer.sign(unsigned.clone()).expect("request should sign");
        let signature = signed
            .params
            .signature
            .expect("signed request must carry a signature");
        assert_eq!(signature.apiKey, api_key);
        let bytes = websocket_unsigned_request_params_to_bytes(&unsigned)
            .expect("params should serialize")
            .expect("spot order should produce bytes");
        let mut mac = Hmac::<Sha256>::new_from_slice(session_key.as_bytes())
            .expect("session key should be a valid HMAC key");
        mac.update(&bytes);
        let expected =
            ByteEncoder::from(ByteEncoding::HexLower).encode(&mac.finalize().into_bytes());
        assert_eq!(signature.signature, expected);
    }

    #[test]
    fn signer_from_logon_response_is_not_signed_with_api_secret() {
        let api_key = "api-key";
        let session_key = "session-secret";
        let response = logon_response(200, api_key, session_key);
        let signer =
            create_signer_from_message(response).expect("logon response should yield a signer");
        let unsigned = unsigned_spot_order_request();
        let signed = signer.sign(unsigned.clone()).expect("request should sign");
        let signature = signed
            .params
            .signature
            .expect("signed request must carry a signature");
        let bytes = websocket_unsigned_request_params_to_bytes(&unsigned)
            .expect("params should serialize")
            .expect("spot order should produce bytes");
        let mut mac = Hmac::<Sha256>::new_from_slice(api_key.as_bytes())
            .expect("api key should be a valid HMAC key");
        mac.update(&bytes);
        let api_secret_signature =
            ByteEncoder::from(ByteEncoding::HexLower).encode(&mac.finalize().into_bytes());
        assert_ne!(signature.signature, api_secret_signature);
    }

    #[test]
    fn failed_logon_response_is_rejected() {
        let response = BinanceWebsocketResponse {
            error: Some(BinanceError {
                code: "-2015".into(),
                msg: "Invalid API-key, IP, or permissions for action".into(),
            }),
            id: "logon-1".into(),
            rateLimits: vec![],
            result: None,
            status: 400,
        };
        let result = create_signer_from_message(response);
        assert!(matches!(result, Err(EGError::AuthenticationFailed(_))));
    }

    #[test]
    fn non_success_logon_status_is_rejected() {
        let response = logon_response(500, "api-key", "session-secret");
        let result = create_signer_from_message(response);
        assert!(matches!(result, Err(EGError::AuthenticationFailed(_))));
    }

    #[test]
    fn logon_response_without_session_result_is_rejected() {
        let response = BinanceWebsocketResponse {
            error: None,
            id: "logon-1".into(),
            rateLimits: vec![],
            result: None,
            status: 200,
        };
        let result = create_signer_from_message(response);
        assert!(matches!(result, Err(EGError::BadResponse)));
    }

    #[test]
    fn credentials_without_session_are_rejected() {
        let credentials = Some(ApiKeyCredentials {
            api_key: "api-key".into(),
            secret: SecretString::from("secret"),
        });
        assert!(matches!(
            validate_use_session(&credentials, false),
            Err(EGError::InvalidConfiguration(_))
        ));
        assert!(validate_use_session(&credentials, true).is_ok());
        assert!(validate_use_session(&None, false).is_ok());
    }
}
