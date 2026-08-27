use crate::{
    authenticate_leg::AuthenticateLeg,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{ArcCombineValues, ArcTryConvertValue},
    rate_limit::{
        feedback::{RateLimitFeedback, RateLimitUsage},
        rate_limit_config::RateLimitConfig,
        rate_limiter::RateLimiter,
        rate_limits::RateLimits,
    },
    sign::{
        convert_signer::ConvertSigner,
        encode::byte_encoding::ByteEncoding,
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
        message_signer::MessageSigner,
        signer::Signer,
    },
    time_sync::TimeSync,
    transports::http::HttpEndpoint,
    urls::{ExchangeTransportUrls, ExchangeUrls},
};
use exchange_types::binance::{
    http::{
        BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
        BinanceHttpUnsignedRequest,
    },
    logon::BinanceLogonParams,
    rate_limits::{BinanceRateLimit, BinanceRateLimitInterval},
    signed::BinanceSignedParams,
    spot::BinanceSpotOrderParams,
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketResponseResult, BinanceWebsocketUnsignedParams,
        BinanceWebsocketUnsignedRequest,
    },
};
use rust_decimal::Decimal;
use secrecy::SecretString;
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};
use uuid::Uuid;

#[cfg(feature = "iris")]
use crate::{
    listeners::websocket_listener::WebsocketListener, transports::iris::IrisWebsocketClient,
    transports::websocket::WebsocketTransport,
};

#[cfg(feature = "reqwest")]
use {
    crate::transports::{
        http::HttpTransport,
        reqwest::{HttpRequest, HttpResponse, ReqwestHttpClient},
    },
    reqwest::Method,
};

#[cfg(any(feature = "reqwest", feature = "iris"))]
use crate::{
    auth_gate::AuthGate,
    connector::Connector,
    connector_impl::ConnectorImpl,
    error::EGError,
    listeners::convert_listener::ConvertListener,
    listeners::listener::ListenerTrait,
    transports::transport::Transport,
    urls::{ExchangeTransportType, TradingMode},
};

/// Binance's default `recvWindow`, used when the caller does not specify one.
const DEFAULT_RECV_WINDOW_MILLIS: u64 = 5000;

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "reqwest")]
pub(crate) fn http_connector<ExternalReq, ExternalRes>(
    trading_mode: TradingMode,
    to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
    to_external_response: ArcTryConvertValue<BinanceHttpResponse, ExternalRes>,
    listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    credentials: Option<ApiKeyCredentials>,
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    ExternalReq: Send,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let exchange_urls = exchange_urls();
    let url = exchange_urls.url(ExchangeTransportType::Http, trading_mode);
    let client = Arc::new(ReqwestHttpClient::new(&url));
    let rate_limits = rate_limits();
    let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceHttpResponse>> =
        Arc::new(ConvertListener::new(to_external_response, listener));
    let http_transport = HttpTransport::new(
        client,
        Arc::new(to_http_request),
        Arc::new(from_http_response),
        response_listener,
        request_to_http_endpoint,
        http_endpoints(),
        rate_limits.clone(),
        http_response_feedback,
    );
    let time_sync = Arc::new(TimeSync::default());
    Ok(ConnectorImpl::new(
        rate_limits,
        http_request_weight,
        http_order_count,
        to_unsigned_request,
        sync_http_timestamp(time_sync),
        Transport::Http(http_transport),
        null_http_signer(),
        credentials,
        create_http_signer_from_credentials,
        vec![],
        Arc::new(AuthGate::default()),
    ))
}
#[cfg(feature = "iris")]
pub(crate) fn websocket_connector<ExternalReq, ExternalRes>(
    trading_mode: TradingMode,
    to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
    listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    credentials: Option<ApiKeyCredentials>,
    use_session: bool,
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    ExternalReq: Send + Sync,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let exchange_urls = exchange_urls();
    let url = exchange_urls.url(ExchangeTransportType::Websocket, trading_mode);
    let rate_limits = rate_limits();
    let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
        Arc::new(ConvertListener::new(to_external_response, listener));
    let auth_gate = Arc::new(AuthGate::default());
    let websocket_listener = Arc::new(WebsocketListener::new(
        Arc::new(from_websocket_response),
        websocket_response_feedback,
        rate_limits.clone(),
        response_listener,
        auth_gate.clone(),
    ));
    let client = Arc::new(IrisWebsocketClient::<
        BinanceWebsocketRequest,
        BinanceWebsocketResponse,
    >::new(&url, websocket_listener.clone()));
    let websocket_transport = WebsocketTransport::new(
        client,
        Arc::new(to_websocket_request),
        Arc::new(from_websocket_response),
        websocket_listener,
    );
    let time_sync = Arc::new(TimeSync::default());
    let authenticate_legs = if use_session {
        let api_key = match &credentials {
            Some(credentials) => credentials.api_key.clone(),
            None => return Err(EGError::NotAuthenticated),
        };
        vec![authenticate_websocket_leg(api_key, time_sync.clone())]
    } else {
        vec![]
    };
    Ok(ConnectorImpl::new(
        rate_limits,
        websocket_request_weight,
        websocket_order_count,
        to_unsigned_request,
        sync_websocket_timestamp(time_sync),
        Transport::Websocket(websocket_transport),
        null_websocket_signer(),
        credentials,
        create_websocket_signer_from_credentials,
        authenticate_legs,
        auth_gate,
    ))
}

/// Builds the transport-level HTTP request from the signed exchange-level request
#[cfg(feature = "reqwest")]
fn to_http_request(request: BinanceHttpRequest) -> EGResult<HttpRequest> {
    let BinanceSignedParams { params, signature } = request;
    let mut headers = Vec::new();
    let (method, query) = match params {
        BinanceHttpUnsignedRequest::ExchangeInfo(_) => (Method::GET, None),
        BinanceHttpUnsignedRequest::AssetLimits(params) => (
            Method::GET,
            Some(signed_query(params.query_params(true), signature)),
        ),
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => {
            let mut params = *params;
            if let Some(api_key) = params.apiKey.take() {
                headers.push(("X-MBX-APIKEY".into(), api_key));
            }
            (
                Method::POST,
                Some(signed_query(params.query_params(true), signature)),
            )
        }
    };
    Ok(HttpRequest {
        method,
        query,
        headers,
        body: None,
    })
}

#[cfg(feature = "reqwest")]
fn signed_query(query: String, signature: Option<String>) -> String {
    match signature {
        Some(signature) => format!("{query}&signature={signature}"),
        None => query,
    }
}

#[cfg(feature = "reqwest")]
fn from_http_response(response: HttpResponse) -> EGResult<BinanceHttpResponse> {
    if response.status == 200 {
        let result = serde_json::from_slice(&response.body)
            .map_err(|error| EGError::External(Box::new(error)))?;
        Ok(BinanceHttpResponse::Result(result))
    } else {
        let error = serde_json::from_slice(&response.body)
            .map_err(|error| EGError::External(Box::new(error)))?;
        Ok(BinanceHttpResponse::Error(error))
    }
}

#[cfg(feature = "iris")]
fn to_websocket_request(request: BinanceWebsocketRequest) -> EGResult<BinanceWebsocketRequest> {
    Ok(request)
}

#[cfg(feature = "iris")]
fn from_websocket_response(
    response: BinanceWebsocketResponse,
) -> EGResult<BinanceWebsocketResponse> {
    Ok(response)
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
        BinanceHttpUnsignedRequest::AssetLimits(..) => HttpEndpoint::AssetLimits,
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

fn authenticate_websocket_leg(
    api_key: String,
    time_sync: Arc<TimeSync>,
) -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let timeout = Duration::from_secs(20);
    let id = id();
    let create_auth_message = {
        let id = id.clone();
        let api_key = api_key.clone();
        let time_sync = time_sync.clone();
        Arc::new(move || websocket_auth_message(&id, &api_key, &time_sync))
    };
    let filter = {
        Arc::new(move |response: &BinanceWebsocketResponse| {
            response.id == *id && response.error.is_none() && response.status == 200
        })
    };
    let create_signer = {
        let time_sync = time_sync.clone();
        Arc::new(
            move |message: BinanceWebsocketResponse| -> EGResult<
                Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>,
            > {
                sync_from_logon_response(&message, &time_sync)?;
                Ok(Box::new(ConvertSigner::new(websocket_converter)))
            },
        )
    };
    AuthenticateLeg {
        create_auth_message,
        create_signer,
        filter,
        timeout,
    }
}
fn websocket_auth_message(
    id: &str,
    api_key: &str,
    time_sync: &TimeSync,
) -> BinanceWebsocketUnsignedRequest {
    let timestamp = time_sync.now_millis();
    let params = BinanceLogonParams {
        apiKey: api_key.to_string(),
        timestamp,
    };
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id: id.to_string(),
            method: BinanceWebsocketMethodName::Logon,
        },
        params: BinanceWebsocketUnsignedParams::Logon(params),
    }
}
fn sync_from_logon_response(
    message: &BinanceWebsocketResponse,
    time_sync: &TimeSync,
) -> EGResult<()> {
    if let Some(BinanceWebsocketResponseResult::SessionAuthentication(result)) = &message.result {
        time_sync.sync(result.serverTime);
    }
    Ok(())
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

/// Fills in a fresh server-synced `timestamp` and a default `recvWindow` on
/// every signed HTTP request before it is signed.
fn sync_http_timestamp(
    time_sync: Arc<TimeSync>,
) -> ArcTryConvertValue<BinanceHttpUnsignedRequest, BinanceHttpUnsignedRequest> {
    Arc::new(move |request| {
        Ok(match request {
            BinanceHttpUnsignedRequest::AssetLimits(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &time_sync);
                BinanceHttpUnsignedRequest::AssetLimits(params)
            }
            BinanceHttpUnsignedRequest::SpotOrderRequest(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &time_sync);
                BinanceHttpUnsignedRequest::SpotOrderRequest(params)
            }
            request @ BinanceHttpUnsignedRequest::ExchangeInfo(..) => request,
        })
    })
}

/// Fills in a fresh server-synced `timestamp` (and default `recvWindow` for
/// orders) on every signed websocket request before it is signed.
fn sync_websocket_timestamp(
    time_sync: Arc<TimeSync>,
) -> ArcTryConvertValue<BinanceWebsocketUnsignedRequest, BinanceWebsocketUnsignedRequest> {
    Arc::new(move |mut request| {
        match &mut request.params {
            BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &time_sync);
            }
            BinanceWebsocketUnsignedParams::Logon(params) => {
                params.timestamp = time_sync.now_millis();
            }
            BinanceWebsocketUnsignedParams::ExchangeInfo(..) => {}
        }
        Ok(request)
    })
}
fn sync_timestamp_fields(
    timestamp: &mut i64,
    recv_window: &mut Option<Decimal>,
    time_sync: &TimeSync,
) {
    *timestamp = time_sync.now_millis();
    if recv_window.is_none() {
        *recv_window = Some(Decimal::from(DEFAULT_RECV_WINDOW_MILLIS));
    }
}

fn create_http_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceHttpUnsignedRequest, BinanceHttpRequest>> {
    let ApiKeyCredentials { secret, .. } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceHttpUnsignedRequest,
        BinanceHttpRequest,
    >::new(
        Arc::new(http_unsigned_request_to_bytes),
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender_http(),
    )))
}
fn create_websocket_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { secret, .. } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        Arc::new(websocket_unsigned_request_params_to_bytes),
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender_websocket(),
    )))
}
fn http_unsigned_request_to_bytes(
    request: &BinanceHttpUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match request {
        BinanceHttpUnsignedRequest::AssetLimits(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => None,
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => {
            let params_without_api_key = if params.apiKey.is_none() {
                Cow::Borrowed(params)
            } else {
                let mut cloned = params.clone();
                cloned.apiKey = None;
                Cow::Owned(cloned)
            };
            Some(params_without_api_key.query_params(true).into_bytes())
        }
    })
}
fn websocket_unsigned_request_params_to_bytes(
    request: &BinanceWebsocketUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match &request.params {
        BinanceWebsocketUnsignedParams::ExchangeInfo(..) => None,
        BinanceWebsocketUnsignedParams::Logon(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
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

fn rate_limits() -> RateLimits {
    RateLimits {
        weight: RateLimiter::new(vec![RateLimitConfig {
            // per IP
            capacity_per_interval: 6000,
            interval_nanos: Duration::from_mins(1).as_nanos(),
        }]),
        orders: RateLimiter::new(vec![
            // per account
            RateLimitConfig {
                capacity_per_interval: 50,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            },
            RateLimitConfig {
                capacity_per_interval: 160_000,
                interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
            },
        ]),
    }
}

fn rate_limit_usage(limit: &BinanceRateLimit) -> Option<RateLimitUsage> {
    let interval_nanos = rate_limit_interval_nanos(limit.interval)? * limit.intervalNum as u128;
    Some(RateLimitUsage {
        interval_nanos,
        used: limit.count.unwrap_or(0).max(0) as u32,
        limit: Some(limit.limit.max(0) as u32),
    })
}
fn rate_limit_interval_nanos(interval: BinanceRateLimitInterval) -> Option<u128> {
    let secs = match interval {
        BinanceRateLimitInterval::SECOND => 1,
        BinanceRateLimitInterval::MINUTE => 60,
        BinanceRateLimitInterval::HOUR => 60 * 60,
        BinanceRateLimitInterval::DAY => 24 * 60 * 60,
    };
    Some(Duration::from_secs(secs).as_nanos())
}
/// `exchangeInfo` returns the rate-limit definitions Binance currently
/// enforces, so the local capacity is updated from the response body rather
/// than from a hard-coded value that may have drifted.
fn http_response_feedback(response: &BinanceHttpResponse) -> EGResult<RateLimitFeedback> {
    let mut feedback = RateLimitFeedback::default();
    if let BinanceHttpResponse::Result(BinanceHttpResponseResult::ExchangeInfo(info)) = response {
        feedback
            .usage
            .extend(info.rateLimits.iter().filter_map(rate_limit_usage));
    }
    Ok(feedback)
}
/// Every WebSocket API response carries the current rate-limit usage, which
/// realigns the local limiters with the server on each message.
fn websocket_response_feedback(response: &BinanceWebsocketResponse) -> EGResult<RateLimitFeedback> {
    let mut feedback = RateLimitFeedback::default();
    feedback
        .usage
        .extend(response.rateLimits.iter().filter_map(rate_limit_usage));
    Ok(feedback)
}
fn http_request_weight(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::AssetLimits(..) => 40,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => 20,
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => order_weight(params),
    }
}
fn websocket_request_weight(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        BinanceWebsocketUnsignedParams::ExchangeInfo(..) => 4,
        BinanceWebsocketUnsignedParams::Logon(..) => 2,
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => order_weight(params),
    }
}
fn http_order_count(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => 1,
        _ => 0,
    }
}
fn websocket_order_count(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(..) => 1,
        _ => 0,
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

fn signature_appender_http()
-> ArcCombineValues<BinanceHttpUnsignedRequest, Option<String>, BinanceHttpRequest> {
    Arc::new(move |unsigned, signature| BinanceHttpRequest {
        params: unsigned,
        signature,
    })
}
fn signature_appender_websocket()
-> ArcCombineValues<BinanceWebsocketUnsignedRequest, Option<String>, BinanceWebsocketRequest> {
    Arc::new(move |unsigned, signature| {
        let BinanceWebsocketUnsignedRequest {
            metadata,
            params: unsigned_params,
        } = unsigned;
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

    #[cfg(feature = "iris")]
    use {
        crate::transports::websocket::WebsocketClientTrait,
        std::sync::atomic::{AtomicBool, Ordering},
    };

    #[cfg(feature = "reqwest")]
    use {
        crate::transports::http::HttpClientTrait,
        exchange_types::binance::http::BinanceHttpResponseResult,
    };

    #[cfg(any(feature = "iris", feature = "reqwest"))]
    use {async_trait::async_trait, secrecy::SecretString, std::sync::Mutex};

    use exchange_types::binance::{
        asset_limits::BinanceAssetLimitsParams,
        error::BinanceError,
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission, BinanceExchangeInfoResult,
            BinanceExchangeInfoSymbolStatus, BinanceOrderType,
        },
        logon::BinanceSessionAuthenticationResult,
        rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceTimeInForce,
        },
    };

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
    fn logon_filter_only_matches_successful_logon_response() {
        let api_key = "api-key";
        let leg = authenticate_websocket_leg(api_key.into(), Arc::new(TimeSync::default()));
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

    #[test]
    fn http_order_signature_payload_matches_rest_rule() {
        // REST signs the query string only: no `apiKey` (it goes in the
        // X-MBX-APIKEY header) and `type` must not be mangled into `r%23type`
        // by the upstream query-params derive.
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        let payload =
            String::from_utf8(http_unsigned_request_to_bytes(&request).unwrap().unwrap()).unwrap();
        assert!(!payload.contains("apiKey"), "payload: {payload}");
        assert!(payload.contains("type=LIMIT"), "payload: {payload}");
        assert!(!payload.contains("r%23type"), "payload: {payload}");
        assert!(
            payload.contains("timestamp=1700000000000"),
            "payload: {payload}"
        );
    }

    #[test]
    fn websocket_order_signature_payload_includes_api_key() {
        // The WebSocket API signs all params except signature, including
        // apiKey, sorted alphabetically.
        let request = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let payload = String::from_utf8(
            websocket_unsigned_request_params_to_bytes(&request)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(payload.contains("apiKey=my-api-key"), "payload: {payload}");
        assert!(payload.contains("type=LIMIT"), "payload: {payload}");
        assert!(!payload.contains("r%23type"), "payload: {payload}");
    }

    #[test]
    fn asset_limits_signature_payload_is_built() {
        // `/api/v3/myFilters` is a signed endpoint; a payload must exist.
        let request = BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
            recvWindow: None,
            symbols: None,
            timestamp: 1700000000000,
        });
        let payload =
            String::from_utf8(http_unsigned_request_to_bytes(&request).unwrap().unwrap()).unwrap();
        assert_eq!(payload, "timestamp=1700000000000");
    }

    #[test]
    fn http_exchange_info_is_unsigned() {
        let request = BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        });
        assert!(http_unsigned_request_to_bytes(&request).unwrap().is_none());
    }

    #[test]
    fn request_weights_match_binance_docs() {
        let exchange_info = BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        });
        assert_eq!(http_request_weight(&exchange_info), 20);
        let asset_limits = BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
            recvWindow: None,
            symbols: None,
            timestamp: 0,
        });
        assert_eq!(http_request_weight(&asset_limits), 40);
        let order = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        assert_eq!(http_request_weight(&order), 1);

        let websocket_logon = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::Logon,
            },
            params: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams {
                apiKey: "k".into(),
                timestamp: 0,
            }),
        };
        assert_eq!(websocket_request_weight(&websocket_logon), 2);
        let websocket_exchange_info = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "2".into(),
                method: BinanceWebsocketMethodName::ExchangeInfo,
            },
            params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
        };
        assert_eq!(websocket_request_weight(&websocket_exchange_info), 4);
    }

    #[test]
    fn order_rate_limit_is_50_per_10_seconds() {
        let limits = rate_limits();
        for _ in 0..50 {
            assert!(limits.orders.did_acquire(1).unwrap());
        }
        assert!(!limits.orders.did_acquire(1).unwrap());
        limits.orders.refund(1).unwrap();
        assert!(limits.orders.did_acquire(1).unwrap());
    }

    #[test]
    fn weight_rate_limit_is_6000_per_minute() {
        let limits = rate_limits();
        for _ in 0..300 {
            assert!(limits.weight.did_acquire(20).unwrap());
        }
        assert!(!limits.weight.did_acquire(1).unwrap());
        limits.weight.refund(20).unwrap();
        assert!(limits.weight.did_acquire(1).unwrap());
    }

    fn rate_limit(
        rate_limit_type: BinanceRateLimitType,
        interval: BinanceRateLimitInterval,
        interval_num: i32,
        limit: i64,
        count: Option<i64>,
    ) -> BinanceRateLimit {
        BinanceRateLimit {
            count,
            interval,
            intervalNum: interval_num,
            limit,
            rateLimitType: rate_limit_type,
        }
    }

    fn exchange_info_result(rate_limits: Vec<BinanceRateLimit>) -> BinanceExchangeInfoResult {
        BinanceExchangeInfoResult {
            exchangeFilters: vec![],
            rateLimits: rate_limits,
            serverTime: 1700000000000,
            symbols: vec![],
            timezone: "UTC".into(),
        }
    }

    #[test]
    fn exchange_info_feedback_reports_limits_and_usage() {
        let response = BinanceHttpResponse::Result(BinanceHttpResponseResult::ExchangeInfo(
            exchange_info_result(vec![
                rate_limit(
                    BinanceRateLimitType::REQUEST_WEIGHT,
                    BinanceRateLimitInterval::MINUTE,
                    1,
                    6000,
                    Some(1200),
                ),
                rate_limit(
                    BinanceRateLimitType::ORDERS,
                    BinanceRateLimitInterval::SECOND,
                    10,
                    50,
                    Some(3),
                ),
            ]),
        ));
        let feedback = http_response_feedback(&response).unwrap();
        assert_eq!(feedback.usage.len(), 2);
        assert_eq!(
            feedback.usage[0].interval_nanos,
            Duration::from_secs(60).as_nanos()
        );
        assert_eq!(feedback.usage[0].used, 1200);
        assert_eq!(feedback.usage[0].limit, Some(6000));
        assert_eq!(
            feedback.usage[1].interval_nanos,
            Duration::from_secs(10).as_nanos()
        );
        assert_eq!(feedback.usage[1].used, 3);
        assert_eq!(feedback.usage[1].limit, Some(50));
    }

    #[test]
    fn websocket_feedback_reports_usage_on_every_response() {
        let response = BinanceWebsocketResponse {
            error: None,
            id: "1".into(),
            rateLimits: vec![
                rate_limit(
                    BinanceRateLimitType::REQUEST_WEIGHT,
                    BinanceRateLimitInterval::MINUTE,
                    1,
                    6000,
                    Some(2500),
                ),
                rate_limit(
                    BinanceRateLimitType::ORDERS,
                    BinanceRateLimitInterval::DAY,
                    1,
                    160_000,
                    Some(12),
                ),
            ],
            result: None,
            status: 200,
        };
        let feedback = websocket_response_feedback(&response).unwrap();
        assert_eq!(feedback.usage.len(), 2);
        assert_eq!(
            feedback.usage[0].interval_nanos,
            Duration::from_secs(60).as_nanos()
        );
        assert_eq!(feedback.usage[0].used, 2500);
        assert_eq!(feedback.usage[0].limit, Some(6000));
        assert_eq!(
            feedback.usage[1].interval_nanos,
            Duration::from_secs(24 * 60 * 60).as_nanos()
        );
        assert_eq!(feedback.usage[1].used, 12);
        assert_eq!(feedback.usage[1].limit, Some(160_000));
    }

    #[test]
    fn rate_limit_usage_maps_binance_intervals() {
        let usage = rate_limit_usage(&rate_limit(
            BinanceRateLimitType::ORDERS,
            BinanceRateLimitInterval::DAY,
            1,
            160_000,
            Some(10),
        ))
        .unwrap();
        assert_eq!(
            usage.interval_nanos,
            Duration::from_secs(24 * 60 * 60).as_nanos()
        );
        assert_eq!(usage.used, 10);
        assert_eq!(usage.limit, Some(160_000));
        assert!(
            rate_limit_usage(&rate_limit(
                BinanceRateLimitType::ORDERS,
                BinanceRateLimitInterval::MINUTE,
                1,
                10,
                None,
            ))
            .is_some()
        );
    }
    /// A scripted websocket client: connecting (and reconnecting) reports
    /// `on_connected` to its listener, logon requests are answered with a
    /// successful response, and every outgoing request is recorded. When a
    /// [`LogonGate`] is configured, logon responses can be held so an
    /// authentication can be observed mid-flight.
    #[cfg(feature = "iris")]
    #[derive(Clone)]
    struct MockWebsocketClient {
        listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
        connected: Arc<AtomicBool>,
        sent: Arc<Mutex<Vec<BinanceWebsocketRequest>>>,
        logon_gate: Option<LogonGate>,
    }

    /// Holds logon responses (when `block` is set) until `release` notifies,
    /// letting a test keep an authentication in flight.
    #[cfg(feature = "iris")]
    #[derive(Clone)]
    struct LogonGate {
        block: Arc<AtomicBool>,
        release: Arc<tokio::sync::Notify>,
    }

    #[cfg(feature = "iris")]
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
            message: Self::TransportReq,
            _timeout: Duration,
        ) -> EGResult<()> {
            self.sent
                .lock()
                .expect("mutex should not be poisoned")
                .push(message.clone());
            if matches!(message.metadata.method, BinanceWebsocketMethodName::Logon) {
                if let Some(gate) = &self.logon_gate
                    && gate.block.load(Ordering::SeqCst)
                {
                    gate.release.notified().await;
                }
                let response = logon_response(message.metadata.id, 200, None);
                self.listener.on_message(response).await?;
            }
            Ok(())
        }
        async fn disconnect(&self) -> EGResult<()> {
            self.connected.store(false, Ordering::SeqCst);
            Ok(())
        }
    }

    #[cfg(feature = "iris")]
    struct IgnoreListener;

    #[async_trait]
    #[cfg(feature = "iris")]
    impl ListenerTrait for IgnoreListener {
        type TMessage = BinanceWebsocketResponse;

        async fn on_message(&self, _message: BinanceWebsocketResponse) -> EGResult<()> {
            Ok(())
        }
    }

    #[cfg(feature = "reqwest")]
    struct IgnoreHttpListener;

    #[async_trait]
    #[cfg(feature = "reqwest")]
    impl ListenerTrait for IgnoreHttpListener {
        type TMessage = BinanceHttpResponse;

        async fn on_message(&self, _message: BinanceHttpResponse) -> EGResult<()> {
            Ok(())
        }
    }

    /// Builds a session-based websocket connector backed by the scripted mock
    /// client, handing the caller a handle to the client so reconnects can be
    /// simulated.
    #[cfg(feature = "iris")]
    fn mock_session_connector(
        client_handle: std::sync::mpsc::Sender<MockWebsocketClient>,
        logon_gate: Option<LogonGate>,
    ) -> EGResult<impl Connector<BinanceWebsocketUnsignedRequest, BinanceWebsocketResponse>> {
        let credentials = ApiKeyCredentials {
            api_key: "api-key".into(),
            secret: SecretString::from("secret"),
        };
        let listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
            Arc::new(IgnoreListener);
        let to_unsigned_request: ArcTryConvertValue<
            BinanceWebsocketUnsignedRequest,
            BinanceWebsocketUnsignedRequest,
        > = Arc::new(Ok);
        let to_external_response: ArcTryConvertValue<
            BinanceWebsocketResponse,
            BinanceWebsocketResponse,
        > = Arc::new(Ok);
        let auth_gate = Arc::new(AuthGate::default());
        let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
            Arc::new(ConvertListener::new(to_external_response, listener));
        let websocket_listener = Arc::new(WebsocketListener::new(
            Arc::new(from_websocket_response),
            response_listener,
            auth_gate.clone(),
        ));
        let mock_client = MockWebsocketClient {
            listener: websocket_listener.clone(),
            connected: Arc::new(AtomicBool::new(false)),
            sent: Arc::new(Mutex::new(Vec::new())),
            logon_gate,
        };
        let _ = client_handle.send(mock_client.clone());
        let client: Arc<
            dyn WebsocketClientTrait<
                    TransportReq = BinanceWebsocketRequest,
                    TransportRes = BinanceWebsocketResponse,
                >,
        > = Arc::new(mock_client);
        let websocket_transport = WebsocketTransport::new(
            client,
            Arc::new(to_websocket_request),
            Arc::new(from_websocket_response),
            websocket_listener,
        );
        let time_sync = Arc::new(TimeSync::default());
        let authenticate_legs = vec![authenticate_websocket_leg(
            credentials.api_key.clone(),
            time_sync.clone(),
        )];
        Ok(ConnectorImpl::new(
            rate_limits(),
            websocket_request_weight,
            websocket_order_count,
            to_unsigned_request,
            sync_websocket_timestamp(time_sync),
            Transport::Websocket(websocket_transport),
            null_websocket_signer(),
            Some(credentials),
            create_websocket_signer_from_credentials,
            authenticate_legs,
            auth_gate,
        ))
    }

    fn order_request() -> BinanceWebsocketUnsignedRequest {
        BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "order-1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        }
    }

    /// A scripted HTTP client: records every outgoing request and answers
    /// with a bare success so signed sends can complete without a network.
    #[cfg(feature = "reqwest")]
    #[derive(Clone)]
    struct MockHttpClient {
        sent: Arc<Mutex<Vec<BinanceHttpRequest>>>,
    }

    #[cfg(feature = "reqwest")]
    #[async_trait]
    impl HttpClientTrait for MockHttpClient {
        type TransportReq = BinanceHttpRequest;
        type TransportRes = BinanceHttpResponse;

        async fn send_message(
            &self,
            _endpoint: &str,
            message: Self::TransportReq,
            _timeout: Duration,
        ) -> EGResult<Self::TransportRes> {
            self.sent.lock().unwrap().push(message);
            Ok(BinanceHttpResponse::Result(
                BinanceHttpResponseResult::AssetLimits(vec![]),
            ))
        }
    }

    /// Builds an HTTP connector backed by the scripted mock client, handing
    /// the caller a handle to the client so sent requests can be inspected.
    #[cfg(feature = "reqwest")]
    fn mock_http_connector(
        client_handle: std::sync::mpsc::Sender<MockHttpClient>,
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
        };
        let _ = client_handle.send(mock_client.clone());
        let client: Arc<
            dyn HttpClientTrait<
                    TransportReq = BinanceHttpRequest,
                    TransportRes = BinanceHttpResponse,
                >,
        > = Arc::new(mock_client);
        let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceHttpResponse>> =
            Arc::new(ConvertListener::new(to_external_response, listener));
        let http_transport = HttpTransport::new(
            client,
            Arc::new(Ok),
            Arc::new(Ok),
            response_listener,
            request_to_http_endpoint,
            http_endpoints(),
        );
        let time_sync = Arc::new(TimeSync::default());
        Ok(ConnectorImpl::new(
            rate_limits(),
            http_request_weight,
            http_order_count,
            to_unsigned_request,
            sync_http_timestamp(time_sync),
            Transport::Http(http_transport),
            null_http_signer(),
            Some(credentials),
            create_http_signer_from_credentials,
            vec![],
            Arc::new(AuthGate::default()),
        ))
    }

    fn asset_limits_request() -> BinanceHttpUnsignedRequest {
        BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
            recvWindow: None,
            symbols: None,
            timestamp: 0,
        })
    }

    #[tokio::test]
    #[cfg(feature = "reqwest")]
    async fn http_connector_installs_signer_on_connect() {
        let (client_tx, client_rx) = std::sync::mpsc::channel();
        let connector = mock_http_connector(client_tx).unwrap();
        let client = client_rx.recv().unwrap();

        connector.connect().await.expect("connect should succeed");
        assert!(
            connector.is_authenticated().unwrap(),
            "connecting with credentials must install the request signer"
        );

        // A signed request must not fail with NotAuthenticated.
        connector
            .send(asset_limits_request(), true, Duration::from_secs(5))
            .await
            .expect("signed send should succeed");
        assert_eq!(client.sent.lock().unwrap().len(), 1);
    }

    fn exchange_info_request() -> BinanceWebsocketUnsignedRequest {
        BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "exchange-info".into(),
                method: BinanceWebsocketMethodName::ExchangeInfo,
            },
            params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
        }
    }

    fn logon_count(sent: &[BinanceWebsocketRequest]) -> usize {
        sent.iter()
            .filter(|message| matches!(message.metadata.method, BinanceWebsocketMethodName::Logon))
            .count()
    }

    #[tokio::test]
    #[cfg(feature = "iris")]
    async fn reauthenticates_after_reconnect() {
        let (client_tx, client_rx) = std::sync::mpsc::channel();
        let connector = mock_session_connector(client_tx, None).unwrap();
        let client = client_rx.recv().unwrap();

        connector.connect().await.expect("connect should succeed");
        assert!(connector.is_authenticated().unwrap());
        assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

        // A signed request on a live connection does not re-authenticate.
        connector
            .send(order_request(), true, Duration::from_secs(5))
            .await
            .expect("send should succeed");
        assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

        // Simulate the connection dropping and the iris client reconnecting.
        client.connected.store(false, Ordering::SeqCst);
        client.connect().await.expect("reconnect should succeed");

        // The old session is stale until re-authentication runs.
        assert!(!connector.is_authenticated().unwrap());

        // The next signed send re-authenticates before sending.
        connector
            .send(order_request(), true, Duration::from_secs(5))
            .await
            .expect("send should succeed");
        assert_eq!(logon_count(&client.sent.lock().unwrap()), 2);
        assert!(connector.is_authenticated().unwrap());
        assert!(client.sent.lock().unwrap().iter().any(|message| matches!(
            message.metadata.method,
            BinanceWebsocketMethodName::PlaceOrder
        )));
    }

    #[tokio::test]
    #[cfg(feature = "iris")]
    async fn logon_weight_counts_against_weight_rate_limit() {
        let (client_tx, _client_rx) = std::sync::mpsc::channel();
        let connector = mock_session_connector(client_tx, None).unwrap();

        connector.connect().await.expect("connect should succeed");

        // The logon consumes 2 of the 6000 weight budget; exchangeInfo costs 4,
        // so exactly 1499 more requests fit in the remaining 5998. If the logon
        // weight were not counted, a 1500th request would still fit.
        for _ in 0..1499 {
            connector
                .send(exchange_info_request(), false, Duration::from_secs(5))
                .await
                .expect("send should succeed");
        }
        let result = connector
            .send(exchange_info_request(), false, Duration::from_secs(5))
            .await;
        assert!(matches!(result, Err(EGError::RateLimited)));
    }

    #[tokio::test]
    #[cfg(feature = "iris")]
    async fn concurrent_sends_wait_for_in_flight_authentication() {
        let (client_tx, client_rx) = std::sync::mpsc::channel();
        let logon_gate = LogonGate {
            block: Arc::new(AtomicBool::new(false)),
            release: Arc::new(tokio::sync::Notify::new()),
        };
        let connector =
            Arc::new(mock_session_connector(client_tx, Some(logon_gate.clone())).unwrap());
        let client = client_rx.recv().unwrap();

        connector.connect().await.expect("connect should succeed");
        assert!(connector.is_authenticated().unwrap());
        assert_eq!(logon_count(&client.sent.lock().unwrap()), 1);

        // Simulate the connection dropping and the iris client reconnecting.
        client.connected.store(false, Ordering::SeqCst);
        client.connect().await.expect("reconnect should succeed");
        assert!(!connector.is_authenticated().unwrap());

        // Hold logon responses so the next re-authentication stays in flight.
        logon_gate.block.store(true, Ordering::SeqCst);

        // The first signed send starts re-authentication and blocks on the
        // held logon response.
        let first = {
            let connector = connector.clone();
            tokio::spawn(async move {
                connector
                    .send(order_request(), true, Duration::from_secs(5))
                    .await
            })
        };
        for _ in 0..100 {
            if logon_count(&client.sent.lock().unwrap()) == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            logon_count(&client.sent.lock().unwrap()),
            2,
            "re-authentication logon should be in flight"
        );

        // Two more signed sends arrive while authentication is in flight:
        // they must wait for it to finish instead of starting a second one.
        let second = {
            let connector = connector.clone();
            tokio::spawn(async move {
                connector
                    .send(order_request(), true, Duration::from_secs(5))
                    .await
            })
        };
        let third = {
            let connector = connector.clone();
            tokio::spawn(async move {
                connector
                    .send(order_request(), true, Duration::from_secs(5))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            logon_count(&client.sent.lock().unwrap()),
            2,
            "concurrent sends must not start a second authentication"
        );

        // Release the in-flight logon: all three sends should now complete.
        logon_gate.block.store(false, Ordering::SeqCst);
        logon_gate.release.notify_one();

        first
            .await
            .expect("first send task should not panic")
            .expect("first send should succeed");
        second
            .await
            .expect("second send task should not panic")
            .expect("second send should succeed");
        third
            .await
            .expect("third send task should not panic")
            .expect("third send should succeed");

        assert_eq!(logon_count(&client.sent.lock().unwrap()), 2);
        assert!(connector.is_authenticated().unwrap());
        let order_count = client
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|message| {
                matches!(
                    message.metadata.method,
                    BinanceWebsocketMethodName::PlaceOrder
                )
            })
            .count();
        assert_eq!(order_count, 3);
    }

    #[test]
    fn time_sync_applies_server_offset() {
        let time_sync = TimeSync::default();
        let local = time_sync.now_millis();
        time_sync.sync(local + 10_000);
        let synced = time_sync.now_millis();
        assert!(synced >= local + 10_000, "synced: {synced}");
        assert!(synced < local + 10_000 + 60_000, "synced: {synced}");
    }

    #[test]
    fn http_sync_timestamp_fills_fresh_timestamp_and_default_recv_window() {
        let time_sync = Arc::new(TimeSync::default());
        let before = time_sync.now_millis();
        let sync = sync_http_timestamp(time_sync);
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        let BinanceHttpUnsignedRequest::SpotOrderRequest(synced) = sync(request).unwrap() else {
            panic!("expected spot order request");
        };
        assert!(
            synced.timestamp >= before,
            "timestamp: {}",
            synced.timestamp
        );
        assert_eq!(synced.recvWindow, Some(Decimal::from(5000u64)));
    }

    #[test]
    fn http_sync_timestamp_preserves_caller_recv_window() {
        let time_sync = Arc::new(TimeSync::default());
        let sync = sync_http_timestamp(time_sync);
        let mut params = spot_order_params();
        params.recvWindow = Some(Decimal::from(10_000u64));
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(params));
        let BinanceHttpUnsignedRequest::SpotOrderRequest(synced) = sync(request).unwrap() else {
            panic!("expected spot order request");
        };
        assert_eq!(synced.recvWindow, Some(Decimal::from(10_000u64)));
    }

    #[test]
    fn http_sync_timestamp_leaves_exchange_info_unchanged() {
        let time_sync = Arc::new(TimeSync::default());
        let sync = sync_http_timestamp(time_sync);
        let request = BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        });
        let synced = sync(request).unwrap();
        assert!(matches!(
            synced,
            BinanceHttpUnsignedRequest::ExchangeInfo(..)
        ));
    }

    #[test]
    fn websocket_sync_timestamp_fills_fresh_timestamp_and_default_recv_window() {
        let time_sync = Arc::new(TimeSync::default());
        let before = time_sync.now_millis();
        let sync = sync_websocket_timestamp(time_sync);
        let request = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let synced = sync(request).unwrap();
        let BinanceWebsocketUnsignedParams::SpotOrderRequest(synced) = synced.params else {
            panic!("expected spot order request");
        };
        assert!(
            synced.timestamp >= before,
            "timestamp: {}",
            synced.timestamp
        );
        assert_eq!(synced.recvWindow, Some(Decimal::from(5000u64)));
    }

    #[test]
    fn logon_response_syncs_server_time() {
        let time_sync = Arc::new(TimeSync::default());
        let leg = authenticate_websocket_leg("api-key".into(), time_sync.clone());
        let local = time_sync.now_millis();
        let response = BinanceWebsocketResponse {
            error: None,
            id: "id".into(),
            rateLimits: vec![],
            result: Some(BinanceWebsocketResponseResult::SessionAuthentication(
                BinanceSessionAuthenticationResult {
                    apiKey: "api-key".into(),
                    authorizedSince: local,
                    connectedSince: local,
                    returnRateLimits: false,
                    serverTime: local + 10_000,
                    userDataStream: false,
                },
            )),
            status: 200,
        };
        let _signer = (leg.create_signer)(response).unwrap();
        assert!(
            time_sync.now_millis() >= local + 10_000,
            "now: {}",
            time_sync.now_millis()
        );
    }
}
