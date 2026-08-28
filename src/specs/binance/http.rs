use crate::{
    auth_gate::AuthGate,
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{ArcCombineValues, ArcTryConvertValue},
    listeners::convert_listener::ConvertListener,
    listeners::listener::ListenerTrait,
    rate_limit::feedback::RateLimitFeedback,
    sign::{
        convert_signer::ConvertSigner, encode::byte_encoding::ByteEncoding,
        message_signer::MessageSigner, signer::Signer,
    },
    specs::binance::common::{data_signer, order_weight, rate_limit_usage, sync_timestamp_fields},
    specs::binance::common::{exchange_urls, rate_limits},
    time_sync::TimeSync,
    transports::http::HttpEndpoint,
    transports::transport::Transport,
    transports::{
        http::HttpTransport,
        reqwest::{HttpRequest, HttpResponse, ReqwestHttpClient},
    },
    urls::{ExchangeTransportType, TradingMode},
};
use exchange_types::binance::{
    exchange_info::BinanceExchangeInfoParams,
    http::{
        BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
        BinanceHttpUnsignedRequest,
    },
    signed::BinanceSignedParams,
};
use reqwest::Method;
use std::{borrow::Cow, collections::HashMap, sync::Arc};

#[allow(clippy::too_many_arguments)]
pub(crate) fn connector<ExternalReq, ExternalRes>(
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
    let transport = HttpTransport::new(
        client,
        Arc::new(to_request),
        Arc::new(from_response),
        response_listener,
        request_to_endpoint,
        endpoints(),
        rate_limits.clone(),
        response_feedback,
    );
    let time_sync = Arc::new(TimeSync::default());
    Ok(ConnectorImpl::new(
        rate_limits,
        request_weight,
        order_count,
        to_unsigned_request,
        sync_timestamp(time_sync),
        Transport::Http(transport),
        null_signer(),
        credentials,
        create_signer_from_credentials,
        vec![],
        Arc::new(AuthGate::default()),
    ))
}

fn to_request(request: BinanceHttpRequest) -> EGResult<HttpRequest> {
    let BinanceSignedParams { params, signature } = request;
    let mut headers = Vec::new();
    let (method, query) = match params {
        BinanceHttpUnsignedRequest::ExchangeInfo(params) => {
            (Method::GET, Some(exchange_info_query(&params)))
        }
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

fn signed_query(query: String, signature: Option<String>) -> String {
    match signature {
        Some(signature) => format!("{query}&signature={signature}"),
        None => query,
    }
}

fn exchange_info_query(params: &BinanceExchangeInfoParams) -> String {
    let mut pairs = Vec::new();
    if !params.permissions.is_empty() {
        pairs.push(format!(
            "permissions={}",
            params
                .permissions
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    pairs.push(format!("symbolStatus={}", params.symbolStatus));
    pairs.join("&")
}

fn from_response(response: HttpResponse) -> EGResult<BinanceHttpResponse> {
    if (200..300).contains(&response.status) {
        let result = serde_json::from_slice(&response.body)
            .map_err(|error| EGError::External(Box::new(error)))?;
        Ok(BinanceHttpResponse::Result(result))
    } else {
        let error = serde_json::from_slice(&response.body)
            .map_err(|error| EGError::External(Box::new(error)))?;
        Ok(BinanceHttpResponse::Error(error))
    }
}

// TODO why is this pub(crate)?
pub(crate) fn request_to_endpoint(request: &BinanceHttpRequest) -> HttpEndpoint {
    match request.params {
        BinanceHttpUnsignedRequest::AssetLimits(..) => HttpEndpoint::AssetLimits,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => HttpEndpoint::ExchangeInfo,
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => HttpEndpoint::PlaceOrder,
    }
}

// TODO why is this pub(crate)?
pub(crate) fn endpoints() -> HashMap<HttpEndpoint, String> {
    let mut endpoints = HashMap::new();
    endpoints.insert(HttpEndpoint::AssetLimits, "myFilters".into());
    endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
    endpoints.insert(HttpEndpoint::PlaceOrder, "order".into());
    endpoints
}

// TODO why is this pub(crate)?
pub(crate) fn null_signer() -> ConvertSigner<BinanceHttpUnsignedRequest, BinanceHttpRequest> {
    ConvertSigner::new(|unsigned| {
        Ok(BinanceHttpRequest {
            params: unsigned,
            signature: None,
        })
    })
}

// TODO why is this pub(crate)?
pub(crate) fn sync_timestamp(
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

// TODO why is this pub(crate)?
pub(crate) fn create_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceHttpUnsignedRequest, BinanceHttpRequest>> {
    let ApiKeyCredentials { secret, .. } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceHttpUnsignedRequest,
        BinanceHttpRequest,
    >::new(
        Arc::new(unsigned_request_to_bytes),
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender(),
    )))
}

fn unsigned_request_to_bytes(request: &BinanceHttpUnsignedRequest) -> EGResult<Option<Vec<u8>>> {
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

// TODO why is this pub(crate)?
pub(crate) fn response_feedback(response: &BinanceHttpResponse) -> EGResult<RateLimitFeedback> {
    let mut feedback = RateLimitFeedback::default();
    if let BinanceHttpResponse::Result(BinanceHttpResponseResult::ExchangeInfo(info)) = response {
        feedback
            .usage
            .extend(info.rateLimits.iter().filter_map(rate_limit_usage));
    }
    Ok(feedback)
}

// TODO why is this pub(crate)?
pub(crate) fn request_weight(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::AssetLimits(..) => 40,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => 20,
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => order_weight(params),
    }
}

// TODO why is this pub(crate)?
pub(crate) fn order_count(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => 1,
        _ => 0,
    }
}

fn signature_appender()
-> ArcCombineValues<BinanceHttpUnsignedRequest, Option<String>, BinanceHttpRequest> {
    Arc::new(move |unsigned, signature| BinanceHttpRequest {
        params: unsigned,
        signature,
    })
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::rate_limit::rate_limit_type::RateLimitType;

    use super::*;
    use exchange_types::binance::{
        asset_limits::BinanceAssetLimitsParams,
        exchange_info::{
            BinanceExchangeInfoPermission, BinanceExchangeInfoResult,
            BinanceExchangeInfoSymbolStatus, BinanceOrderType,
        },
        rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
    };
    use rust_decimal::Decimal;

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
    fn order_signature_payload_matches_rest_rule() {
        // REST signs the query string only: no `apiKey` (it goes in the
        // X-MBX-APIKEY header) and `type` must not be mangled into `r%23type`
        // by the upstream query-params derive.
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        let payload =
            String::from_utf8(unsigned_request_to_bytes(&request).unwrap().unwrap()).unwrap();
        assert!(!payload.contains("apiKey"), "payload: {payload}");
        assert!(payload.contains("type=LIMIT"), "payload: {payload}");
        assert!(!payload.contains("r%23type"), "payload: {payload}");
        assert!(
            payload.contains("timestamp=1700000000000"),
            "payload: {payload}"
        );
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
            String::from_utf8(unsigned_request_to_bytes(&request).unwrap().unwrap()).unwrap();
        assert_eq!(payload, "timestamp=1700000000000");
    }

    #[test]
    fn exchange_info_is_unsigned() {
        let request = BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        });
        assert!(unsigned_request_to_bytes(&request).unwrap().is_none());
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn exchange_info_query_is_forwarded() {
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
            signature: None,
        };
        let request = to_request(request).unwrap();
        assert_eq!(request.method, Method::GET);
        assert_eq!(
            request.query.as_deref(),
            Some("permissions=SPOT&symbolStatus=TRADING")
        );
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn exchange_info_omits_empty_permissions() {
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
            signature: None,
        };
        let request = to_request(request).unwrap();
        assert_eq!(request.query.as_deref(), Some("symbolStatus=TRADING"));
    }

    #[test]
    fn exchange_info_feedback_adopts_limits_without_usage() {
        // REST exchangeInfo rateLimits entries carry the current limit
        // definitions but never a usage count (only WebSocket API responses
        // include `count`), so the feedback must adopt the limits without
        // reporting any usage: locally-consumed capacity stays untouched.
        let response = BinanceHttpResponse::Result(BinanceHttpResponseResult::ExchangeInfo(
            exchange_info_result(vec![
                rate_limit(
                    BinanceRateLimitType::REQUEST_WEIGHT,
                    BinanceRateLimitInterval::MINUTE,
                    1,
                    6000,
                    None,
                ),
                rate_limit(
                    BinanceRateLimitType::ORDERS,
                    BinanceRateLimitInterval::SECOND,
                    10,
                    50,
                    None,
                ),
            ]),
        ));
        let feedback = response_feedback(&response).unwrap();
        assert_eq!(feedback.usage.len(), 2);
        assert_eq!(
            feedback.usage[0].rate_limit_type,
            RateLimitType::RequestWeight
        );
        assert_eq!(
            feedback.usage[0].interval_nanos,
            Duration::from_secs(60).as_nanos()
        );
        assert_eq!(feedback.usage[0].used, None);
        assert_eq!(feedback.usage[0].limit, Some(6000));
        assert_eq!(feedback.usage[1].rate_limit_type, RateLimitType::Orders);
        assert_eq!(
            feedback.usage[1].interval_nanos,
            Duration::from_secs(10).as_nanos()
        );
        assert_eq!(feedback.usage[1].used, None);
        assert_eq!(feedback.usage[1].limit, Some(50));
    }

    #[test]
    fn request_weight_and_raw_requests_share_the_minute_window_without_overwriting() {
        // Binance reports REQUEST_WEIGHT (6000/min) and RAW_REQUESTS
        // (61000/min) with the same one-minute window, RAW_REQUESTS last.
        // Both usages must not be collapsed onto the single weight limiter:
        // the raw-requests limit must not overwrite the weight bucket's.
        let limits = rate_limits();
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
                    BinanceRateLimitType::RAW_REQUESTS,
                    BinanceRateLimitInterval::MINUTE,
                    1,
                    61000,
                    Some(40_000),
                ),
            ]),
        ));
        let feedback = response_feedback(&response).unwrap();
        limits.apply_feedback(&feedback).unwrap();
        // The weight limiter keeps the request-weight limit: 4800 remaining,
        // not the raw-requests 61000.
        assert!(limits.weight.did_acquire(4800).unwrap());
        assert!(!limits.weight.did_acquire(1).unwrap());
    }

    #[test]
    fn sync_timestamp_leaves_exchange_info_unchanged() {
        let time_sync = Arc::new(TimeSync::default());
        let sync = sync_timestamp(time_sync);
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
    fn sync_timestamp_preserves_caller_recv_window() {
        let time_sync = Arc::new(TimeSync::default());
        let sync = sync_timestamp(time_sync);
        let mut params = spot_order_params();
        params.recvWindow = Some(Decimal::from(10_000u64));
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(params));
        let BinanceHttpUnsignedRequest::SpotOrderRequest(synced) = sync(request).unwrap() else {
            panic!("expected spot order request");
        };
        assert_eq!(synced.recvWindow, Some(Decimal::from(10_000u64)));
    }

    #[test]
    fn http_sync_timestamp_fills_fresh_timestamp_and_default_recv_window() {
        let time_sync = Arc::new(TimeSync::default());
        let before = time_sync.now_millis();
        let sync = sync_timestamp(time_sync);
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
    #[cfg(feature = "reqwest")]
    fn from_http_response_parses_non_2xx_as_error() {
        let response = HttpResponse {
            status: 400,
            body: br#"{"code":-2014,"msg":"API-key format invalid."}"#.to_vec(),
            headers: vec![],
        };
        let parsed = from_response(response).expect("400 should parse as an error");
        match parsed {
            BinanceHttpResponse::Error(error) => {
                assert_eq!(error.code, -2014);
                assert_eq!(error.msg, "API-key format invalid.");
            }
            other => panic!("expected Error, got: {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn from_http_response_parses_any_2xx_as_result() {
        let response = HttpResponse {
            status: 201,
            body: br#"[]"#.to_vec(),
            headers: vec![],
        };
        let parsed = from_response(response).expect("201 should parse as a result");
        assert!(matches!(
            parsed,
            BinanceHttpResponse::Result(BinanceHttpResponseResult::AssetLimits(ref filters))
                if filters.is_empty()
        ));
    }

    #[test]
    fn request_weights_match_binance_docs() {
        let exchange_info = BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        });
        assert_eq!(request_weight(&exchange_info), 20);
        let asset_limits = BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
            recvWindow: None,
            symbols: None,
            timestamp: 0,
        });
        assert_eq!(request_weight(&asset_limits), 40);
        let order = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        assert_eq!(request_weight(&order), 1);
    }
}
