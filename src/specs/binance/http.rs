use crate::{
    auth_gate::AuthGate,
    clock::{Clock, Synchronization},
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{ArcCombineValues, ArcPredicate, ArcTryConvertValue, TryConvertValue},
    rate_limit::feedback::RateLimitFeedback,
    sign::{
        convert_signer::ConvertSigner, encode::byte_encoding::ByteEncoding,
        message_signer::MessageSigner, signer::Signer,
    },
    specs::binance::common::{
        data_signer, exchange_urls, rate_limit_usage, rate_limits, sync_timestamp_fields,
    },
    transports::{
        http::{HttpClientTrait, HttpEndpoint, HttpTransport},
        reqwest::{HttpRequest, HttpResponse},
        transport::Transport,
    },
    urls::{ExchangeTransportType, TradingMode},
};
use exchange_types::binance::{
    amend::BinanceAmendOrderParams,
    cancel::{BinanceCancelAllOrdersParams, BinanceCancelOrderParams},
    exchange_info::BinanceExchangeInfoParams,
    http::{
        BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
        BinanceHttpUnsignedRequest,
    },
    signed::BinanceSignedParams,
    spot::BinanceSpotOrderParams,
    time::BinanceTimeParams,
};
use reqwest::Method;
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

pub(crate) fn connector<ExternalReq, ExternalRes>(
    trading_mode: TradingMode,
    to_unsigned_request: TryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
    to_external_response: TryConvertValue<BinanceHttpResponse, ExternalRes>,
    credentials: Option<ApiKeyCredentials>,
    clock: Clock,
    client_creator: ArcTryConvertValue<
        String,
        impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
    >,
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    ExternalReq: Send,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let url = exchange_urls().url(ExchangeTransportType::Http, trading_mode);
    let client = Arc::new(client_creator(url)?);
    let rate_limits = rate_limits();
    let api_key = credentials
        .as_ref()
        .map(|credentials| credentials.api_key.clone());
    let convert_request =
        Arc::new(move |request: BinanceHttpRequest| to_request(request, api_key.as_deref()));
    let transport = HttpTransport::new(
        client,
        convert_request,
        from_response,
        request_to_endpoint,
        endpoints(),
        rate_limits.clone(),
        response_feedback,
    );
    let authenticate_legs = vec![];
    Ok(ConnectorImpl::new(
        rate_limits,
        clock,
        synchronization(Duration::from_secs(20)),
        to_unsigned_request,
        request_weight,
        order_count,
        sync_timestamp(),
        to_filter,
        Arc::new(to_external_response),
        Transport::Http(transport),
        null_signer(),
        credentials,
        create_signer_from_credentials,
        authenticate_legs,
        Arc::new(AuthGate::default()),
    ))
}

fn to_filter(
    request: BinanceHttpUnsignedRequest,
) -> (
    BinanceHttpUnsignedRequest,
    ArcPredicate<BinanceHttpResponse>,
) {
    (request, Arc::new(|_: &BinanceHttpResponse| true))
}

fn to_request(request: BinanceHttpRequest, api_key: Option<&str>) -> EGResult<HttpRequest> {
    let BinanceSignedParams { params, signature } = request;
    let mut headers = Vec::new();
    let mut set_api_key_header = |request_api_key: Option<String>| {
        if let Some(api_key) = api_key.or(request_api_key.as_deref()) {
            headers.push(("X-MBX-APIKEY".into(), api_key.into()));
        }
    };
    let (method, query) = match params {
        BinanceHttpUnsignedRequest::ExchangeInfo(params) => {
            (Method::GET, Some(exchange_info_query(&params)))
        }
        BinanceHttpUnsignedRequest::AssetLimits(params) => {
            set_api_key_header(None);
            (
                Method::GET,
                Some(signed_query(params.query_params(true), signature)),
            )
        }
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => {
            let mut params = *params;
            set_api_key_header(params.apiKey.take());
            (
                Method::POST,
                Some(signed_query(params.query_params(true), signature)),
            )
        }
        BinanceHttpUnsignedRequest::AmendOrderRequest(mut params) => {
            set_api_key_header(params.apiKey.take());
            (
                Method::POST,
                Some(signed_query(params.query_params(true), signature)),
            )
        }
        BinanceHttpUnsignedRequest::CancelAllOrdersRequest(mut params) => {
            set_api_key_header(params.apiKey.take());
            (
                Method::DELETE,
                Some(signed_query(params.query_params(true), signature)),
            )
        }
        BinanceHttpUnsignedRequest::CancelOrderRequest(mut params) => {
            set_api_key_header(params.apiKey.take());
            (
                Method::DELETE,
                Some(signed_query(params.query_params(true), signature)),
            )
        }
        BinanceHttpUnsignedRequest::Time(..) => (Method::GET, None),
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
        let result: BinanceHttpResponse = serde_json::from_slice(&response.body)
            .map_err(|error| EGError::External(Box::new(error)))?;
        match result {
            BinanceHttpResponse::Success(response) => Ok(BinanceHttpResponse::Success(response)),
            BinanceHttpResponse::Failure(error) => Err(EGError::ApiError {
                code: error.code,
                message: error.msg,
            }),
        }
    } else {
        Err(EGError::HttpError {
            status: response.status,
            body: response.body,
        })
    }
}

fn request_to_endpoint(request: &BinanceHttpRequest) -> HttpEndpoint {
    match request.params {
        BinanceHttpUnsignedRequest::AmendOrderRequest(..) => HttpEndpoint::AmendOrder,
        BinanceHttpUnsignedRequest::AssetLimits(..) => HttpEndpoint::AssetLimits,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => HttpEndpoint::ExchangeInfo,
        BinanceHttpUnsignedRequest::CancelAllOrdersRequest(..) => HttpEndpoint::CancelAllOrders,
        BinanceHttpUnsignedRequest::CancelOrderRequest(..) => HttpEndpoint::CancelOrder,
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => HttpEndpoint::PlaceOrder,
        BinanceHttpUnsignedRequest::Time(..) => HttpEndpoint::Time,
    }
}

fn endpoints() -> HashMap<HttpEndpoint, String> {
    let mut endpoints = HashMap::new();
    endpoints.insert(HttpEndpoint::AmendOrder, "order/amend/keepPriority".into());
    endpoints.insert(HttpEndpoint::AssetLimits, "myFilters".into());
    endpoints.insert(HttpEndpoint::CancelAllOrders, "openOrders".into());
    endpoints.insert(HttpEndpoint::CancelOrder, "order".into());
    endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
    endpoints.insert(HttpEndpoint::PlaceOrder, "order".into());
    endpoints.insert(HttpEndpoint::Time, "time".into());
    endpoints
}

fn null_signer() -> ConvertSigner<BinanceHttpUnsignedRequest, BinanceHttpRequest> {
    ConvertSigner::new(|unsigned| {
        Ok(BinanceHttpRequest {
            params: unsigned,
            signature: None,
        })
    })
}

fn sync_timestamp() -> TryConvertValue<(BinanceHttpUnsignedRequest, i64), BinanceHttpUnsignedRequest>
{
    move |(request, server_time)| {
        Ok(match request {
            BinanceHttpUnsignedRequest::AssetLimits(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
                BinanceHttpUnsignedRequest::AssetLimits(params)
            }
            BinanceHttpUnsignedRequest::SpotOrderRequest(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
                BinanceHttpUnsignedRequest::SpotOrderRequest(params)
            }
            BinanceHttpUnsignedRequest::AmendOrderRequest(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
                BinanceHttpUnsignedRequest::AmendOrderRequest(params)
            }
            BinanceHttpUnsignedRequest::CancelAllOrdersRequest(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
                BinanceHttpUnsignedRequest::CancelAllOrdersRequest(params)
            }
            BinanceHttpUnsignedRequest::CancelOrderRequest(mut params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
                BinanceHttpUnsignedRequest::CancelOrderRequest(params)
            }
            request @ BinanceHttpUnsignedRequest::ExchangeInfo(..) => request,
            request @ BinanceHttpUnsignedRequest::Time(..) => request,
        })
    }
}

fn create_signer_from_credentials(
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
            let params_without_api_key = strip_api_key(params.as_ref());
            Some(params_without_api_key.query_params(true).into_bytes())
        }
        BinanceHttpUnsignedRequest::AmendOrderRequest(params) => {
            let params_without_api_key = strip_api_key(params);
            Some(params_without_api_key.query_params(true).into_bytes())
        }
        BinanceHttpUnsignedRequest::CancelAllOrdersRequest(params) => {
            let params_without_api_key = strip_api_key(params);
            Some(params_without_api_key.query_params(true).into_bytes())
        }
        BinanceHttpUnsignedRequest::CancelOrderRequest(params) => {
            let params_without_api_key = strip_api_key(params);
            Some(params_without_api_key.query_params(true).into_bytes())
        }
        BinanceHttpUnsignedRequest::Time(..) => None,
    })
}

fn strip_api_key<T>(params: &T) -> Cow<'_, T>
where
    T: Clone + HasApiKey,
{
    if params.api_key().is_none() {
        Cow::Borrowed(params)
    } else {
        let mut cloned = params.clone();
        cloned.set_api_key(None);
        Cow::Owned(cloned)
    }
}

trait HasApiKey {
    fn api_key(&self) -> &Option<String>;
    fn set_api_key(&mut self, api_key: Option<String>);
}

impl HasApiKey for BinanceSpotOrderParams {
    fn api_key(&self) -> &Option<String> {
        &self.apiKey
    }
    fn set_api_key(&mut self, api_key: Option<String>) {
        self.apiKey = api_key;
    }
}

impl HasApiKey for BinanceAmendOrderParams {
    fn api_key(&self) -> &Option<String> {
        &self.apiKey
    }
    fn set_api_key(&mut self, api_key: Option<String>) {
        self.apiKey = api_key;
    }
}

impl HasApiKey for BinanceCancelAllOrdersParams {
    fn api_key(&self) -> &Option<String> {
        &self.apiKey
    }
    fn set_api_key(&mut self, api_key: Option<String>) {
        self.apiKey = api_key;
    }
}

impl HasApiKey for BinanceCancelOrderParams {
    fn api_key(&self) -> &Option<String> {
        &self.apiKey
    }
    fn set_api_key(&mut self, api_key: Option<String>) {
        self.apiKey = api_key;
    }
}

fn synchronization(
    timeout: Duration,
) -> Synchronization<BinanceHttpUnsignedRequest, BinanceHttpResponse> {
    let create_time_request = || {
        let message = BinanceHttpUnsignedRequest::Time(BinanceTimeParams {});
        let filter: ArcPredicate<BinanceHttpResponse> = Arc::new(|response| {
            matches!(
                response,
                BinanceHttpResponse::Success(BinanceHttpResponseResult::Time(..))
                    | BinanceHttpResponse::Failure(..)
            )
        });
        (message, filter)
    };
    let to_server_time = |response: &BinanceHttpResponse| -> EGResult<i64> {
        match response {
            BinanceHttpResponse::Success(BinanceHttpResponseResult::Time(result)) => {
                Ok(result.serverTime)
            }
            BinanceHttpResponse::Failure(error) => Err(EGError::ApiError {
                code: error.code,
                message: error.msg.clone(),
            }),
            _ => Err(EGError::BadResponse),
        }
    };
    Synchronization {
        create_time_request,
        timeout,
        to_server_time,
    }
}

fn response_feedback(response: &BinanceHttpResponse) -> EGResult<RateLimitFeedback> {
    let mut feedback = RateLimitFeedback::default();
    if let BinanceHttpResponse::Success(BinanceHttpResponseResult::ExchangeInfo(info)) = response {
        feedback
            .usage
            .extend(info.rateLimits.iter().filter_map(rate_limit_usage));
    }
    Ok(feedback)
}

fn request_weight(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::AmendOrderRequest(..) => 4,
        BinanceHttpUnsignedRequest::AssetLimits(..) => 40,
        BinanceHttpUnsignedRequest::CancelAllOrdersRequest(..) => 1,
        BinanceHttpUnsignedRequest::CancelOrderRequest(..) => 1,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => 20,
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => 1,
        BinanceHttpUnsignedRequest::Time(..) => 1,
    }
}

fn order_count(request: &BinanceHttpUnsignedRequest) -> u32 {
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
        amend::BinanceAmendOrderParams,
        asset_limits::BinanceAssetLimitsParams,
        cancel::{BinanceCancelAllOrdersParams, BinanceCancelOrderParams},
        error::BinanceError,
        exchange_info::{
            BinanceExchangeInfoPermission, BinanceExchangeInfoResult,
            BinanceExchangeInfoSymbolStatus, BinanceOrderType,
        },
        rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
        time::BinanceTimeResult,
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
            symbol: "BNBUSDT".into(),
            timestamp: 1700000000000,
        });
        let payload =
            String::from_utf8(unsigned_request_to_bytes(&request).unwrap().unwrap()).unwrap();
        assert_eq!(payload, "symbol=BNBUSDT&timestamp=1700000000000");
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
        let request = to_request(request, None).unwrap();
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
        let request = to_request(request, None).unwrap();
        assert_eq!(request.query.as_deref(), Some("symbolStatus=TRADING"));
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn asset_limits_request_carries_the_api_key_header() {
        // `/api/v3/myFilters` is a USER_DATA endpoint: the signed query must
        // be accompanied by the X-MBX-APIKEY header taken from the
        // connector's credentials (the params carry no `apiKey` field).
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
                recvWindow: None,
                symbol: "BNBUSDT".into(),
                timestamp: 1700000000000,
            }),
            signature: Some("signature".into()),
        };
        let request = to_request(request, Some("my-api-key")).unwrap();
        assert_eq!(request.method, Method::GET);
        assert!(
            request
                .headers
                .contains(&("X-MBX-APIKEY".into(), "my-api-key".into())),
            "headers: {:?}",
            request.headers
        );
        assert_eq!(
            request.query.as_deref(),
            Some("symbol=BNBUSDT&timestamp=1700000000000&signature=signature")
        );
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn asset_limits_request_without_credentials_omits_the_api_key_header() {
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
                recvWindow: None,
                symbol: "BNBUSDT".into(),
                timestamp: 1700000000000,
            }),
            signature: Some("signature".into()),
        };
        let request = to_request(request, None).unwrap();
        assert!(
            !request
                .headers
                .iter()
                .any(|(name, _)| name == "X-MBX-APIKEY"),
            "headers: {:?}",
            request.headers
        );
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn order_request_with_credentials_overwrites_the_params_api_key() {
        // The connector's credentials are the authoritative api key: a key
        // the caller left on the params is overwritten in the X-MBX-APIKEY
        // header (the signature is built from the connector's secret, so the
        // header must match it) and never leaks into the query string.
        let mut params = spot_order_params();
        params.apiKey = Some("caller-api-key".into());
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(params)),
            signature: Some("signature".into()),
        };
        let request = to_request(request, Some("connector-api-key")).unwrap();
        assert_eq!(request.method, Method::POST);
        assert!(
            request
                .headers
                .contains(&("X-MBX-APIKEY".into(), "connector-api-key".into())),
            "headers: {:?}",
            request.headers
        );
        assert!(
            !request.query.as_deref().unwrap().contains("apiKey"),
            "query: {:?}",
            request.query
        );
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn signed_requests_without_params_api_key_carry_the_connector_key() {
        // The REST footgun: order/cancel/amend previously required the
        // caller to set apiKey on the params; with credentials configured
        // the connector now supplies the X-MBX-APIKEY header itself.
        let mut spot = spot_order_params();
        spot.apiKey = None;
        let requests = vec![
            BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot)),
            BinanceHttpUnsignedRequest::AmendOrderRequest(BinanceAmendOrderParams {
                apiKey: None,
                newClientOrderId: None,
                newQty: Decimal::from(1),
                orderId: Some(1),
                origClientOrderId: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 1700000000000,
            }),
            BinanceHttpUnsignedRequest::CancelAllOrdersRequest(BinanceCancelAllOrdersParams {
                apiKey: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 1700000000000,
            }),
            BinanceHttpUnsignedRequest::CancelOrderRequest(BinanceCancelOrderParams {
                apiKey: None,
                cancelRestrictions: None,
                newClientOrderId: None,
                orderId: Some(1),
                origClientOrderId: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 1700000000000,
            }),
        ];
        for request in requests {
            let request = BinanceHttpRequest {
                params: request,
                signature: Some("signature".into()),
            };
            let request = to_request(request, Some("connector-api-key")).unwrap();
            assert!(
                request
                    .headers
                    .contains(&("X-MBX-APIKEY".into(), "connector-api-key".into())),
                "headers: {:?}",
                request.headers
            );
        }
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn order_request_without_credentials_falls_back_to_the_params_api_key() {
        // Without connector credentials the per-request apiKey is still
        // honoured, so callers can keep supplying keys per request.
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params())),
            signature: Some("signature".into()),
        };
        let request = to_request(request, None).unwrap();
        assert!(
            request
                .headers
                .contains(&("X-MBX-APIKEY".into(), "my-api-key".into())),
            "headers: {:?}",
            request.headers
        );
    }

    #[test]
    #[cfg(feature = "reqwest")]
    fn order_request_without_credentials_or_params_api_key_omits_the_header() {
        let mut params = spot_order_params();
        params.apiKey = None;
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(params)),
            signature: Some("signature".into()),
        };
        let request = to_request(request, None).unwrap();
        assert!(
            !request
                .headers
                .iter()
                .any(|(name, _)| name == "X-MBX-APIKEY"),
            "headers: {:?}",
            request.headers
        );
    }

    #[test]
    fn exchange_info_feedback_adopts_limits_without_usage() {
        // REST exchangeInfo rateLimits entries carry the current limit
        // definitions but never a usage count (only WebSocket API responses
        // include `count`), so the feedback must adopt the limits without
        // reporting any usage: locally-consumed capacity stays untouched.
        let response = BinanceHttpResponse::Success(BinanceHttpResponseResult::ExchangeInfo(
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
        let response = BinanceHttpResponse::Success(BinanceHttpResponseResult::ExchangeInfo(
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
        let clock = Arc::new(Clock::default());
        let sync = sync_timestamp();
        let request = BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
            permissions: vec![BinanceExchangeInfoPermission::SPOT],
            symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
        });
        let synced = sync((request, clock.now_millis())).unwrap();
        assert!(matches!(
            synced,
            BinanceHttpUnsignedRequest::ExchangeInfo(..)
        ));
    }

    #[test]
    fn sync_timestamp_preserves_caller_recv_window() {
        let clock = Arc::new(Clock::default());
        let sync = sync_timestamp();
        let mut params = spot_order_params();
        params.recvWindow = Some(Decimal::from(10_000u64));
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(params));
        let BinanceHttpUnsignedRequest::SpotOrderRequest(synced) =
            sync((request, clock.now_millis())).unwrap()
        else {
            panic!("expected spot order request");
        };
        assert_eq!(synced.recvWindow, Some(Decimal::from(10_000u64)));
    }

    #[test]
    fn http_sync_timestamp_fills_fresh_timestamp_and_default_recv_window() {
        let clock = Arc::new(Clock::default());
        let before = clock.now_millis();
        let sync = sync_timestamp();
        let request = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        let BinanceHttpUnsignedRequest::SpotOrderRequest(synced) =
            sync((request, clock.now_millis())).unwrap()
        else {
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
        let parsed = from_response(response);
        match parsed {
            Err(EGError::HttpError { status, body: _ }) => {
                assert_eq!(status, 400);
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
            BinanceHttpResponse::Success(BinanceHttpResponseResult::AssetLimits(ref filters))
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
            symbol: "BNBUSDT".into(),
            timestamp: 0,
        });
        assert_eq!(request_weight(&asset_limits), 40);
        let order = BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params()));
        assert_eq!(request_weight(&order), 1);
        let time = BinanceHttpUnsignedRequest::Time(BinanceTimeParams {});
        assert_eq!(request_weight(&time), 1);
    }

    #[test]
    fn time_request_is_unsigned_and_routed_to_the_time_endpoint() {
        let request = BinanceHttpRequest {
            params: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
            signature: None,
        };
        // GET /api/v3/time with no query string and nothing to sign.
        let transport_request = to_request(request, None).unwrap();
        assert_eq!(transport_request.method, Method::GET);
        assert_eq!(transport_request.query, None);
        assert!(
            unsigned_request_to_bytes(&BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            request_to_endpoint(&BinanceHttpRequest {
                params: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
                signature: None,
            }),
            HttpEndpoint::Time
        );
        assert_eq!(endpoints().get(&HttpEndpoint::Time).unwrap(), "time");
    }

    #[test]
    fn sync_timestamp_leaves_time_unchanged() {
        let clock = Arc::new(Clock::default());
        let sync = sync_timestamp();
        let request = BinanceHttpUnsignedRequest::Time(BinanceTimeParams {});
        assert!(matches!(
            sync((request, clock.now_millis())).unwrap(),
            BinanceHttpUnsignedRequest::Time(..)
        ));
    }

    #[test]
    fn sync_clock_syncs_the_server_clock() {
        let clock = Arc::new(Clock::default());
        let synchronization = synchronization(Duration::from_secs(20));
        let (message, filter) = (synchronization.create_time_request)();
        assert!(matches!(message, BinanceHttpUnsignedRequest::Time(..)));
        let local = clock.now_millis();
        let response =
            BinanceHttpResponse::Success(BinanceHttpResponseResult::Time(BinanceTimeResult {
                serverTime: local + 10_000,
            }));
        assert!(filter(&response));
        let server_time =
            (synchronization.to_server_time)(&response).expect("No server time from response");
        clock.sync(server_time, Duration::ZERO);
        assert!(
            clock.now_millis() >= local + 10_000,
            "now: {}",
            clock.now_millis()
        );
    }

    #[test]
    fn sync_clock_surfaces_the_time_error() {
        let synchronization = synchronization(Duration::from_secs(20));
        let (_, filter) = (synchronization.create_time_request)();
        let response = BinanceHttpResponse::Failure(BinanceError {
            code: -1021,
            msg: "Timestamp for this request is outside of the recvWindow.".into(),
        });
        assert!(filter(&response));
        let result = (synchronization.to_server_time)(&response);
        assert!(result.is_err(), "expected ApiError");
        let Err(EGError::ApiError { code, message }) = result else {
            panic!("expected an ApiError");
        };
        assert_eq!(code, -1021);
        assert!(message.contains("recvWindow"));
    }
}
