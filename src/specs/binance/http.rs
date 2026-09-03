use crate::{
    clock::{Clock, Synchronization},
    connector::Connector,
    connector_impl::ConnectorImpl,
    error::{EGError, EGResult},
    functions::{ArcPredicate, BoxTryCreateOnce},
    rate_limit::{
        feedback::{RateLimitFeedback, RateLimitUsage},
        rate_limit_type::RateLimitType,
    },
    specs::binance::common::{exchange_urls, rate_limit_usage, rate_limits},
    transports::{
        http::{HttpClientTrait, HttpRequest, HttpResponse, HttpTransport},
        transport::Transport,
    },
    urls::{ExchangeTransportType, TradingMode},
};
use exchange_types::{
    binance::{
        http::{
            BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
            BinanceHttpUnsignedRequest,
        },
        time::BinanceTimeParams,
    },
    http::IntoHttpRequest,
    rate_limited::RateLimited,
};
use std::{sync::Arc, time::Duration};

pub(crate) fn connector(
    trading_mode: TradingMode,
    clock: Clock,
    client_creator: BoxTryCreateOnce<
        String,
        impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
    >,
) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
    let url = exchange_urls().url(ExchangeTransportType::Http, trading_mode);
    let client = Arc::new(client_creator(url)?);
    let rate_limits = rate_limits();
    let transport = HttpTransport::new(
        client,
        Arc::new(to_request),
        from_response,
        rate_limits.clone(),
        http_header_feedback,
        response_feedback,
    );
    Ok(ConnectorImpl::new(
        rate_limits,
        clock,
        synchronization(Duration::from_secs(20)),
        request_weight,
        order_count,
        to_filter,
        Transport::Http(transport),
    ))
}

fn to_filter(_request: &BinanceHttpRequest) -> ArcPredicate<BinanceHttpResponse> {
    Arc::new(|_: &BinanceHttpResponse| true)
}

fn to_request(request: BinanceHttpRequest) -> EGResult<HttpRequest> {
    // `IntoHttpRequest` turns the exchange request into the transport
    // request: the origin-form request target (endpoint and query
    // parameters), the X-MBX-APIKEY header and the body are all derived
    // from the request and its signature, so the transport needs nothing
    // from the exchange request itself.
    Ok(request.into_http_request())
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

fn http_header_feedback(response: &HttpResponse) -> RateLimitFeedback {
    let mut feedback = RateLimitFeedback::default();
    if let Some(used) = parse_header(&response.headers, "x-mbx-used-weight-1m") {
        feedback.usage.push(RateLimitUsage {
            rate_limit_type: RateLimitType::RequestWeight,
            interval_nanos: Duration::from_secs(60).as_nanos(),
            used: Some(used),
            limit: None,
        });
    }
    if let Some(used) = parse_header(&response.headers, "x-mbx-order-count-10s") {
        feedback.usage.push(RateLimitUsage {
            rate_limit_type: RateLimitType::Orders,
            interval_nanos: Duration::from_secs(10).as_nanos(),
            used: Some(used),
            limit: None,
        });
    }
    if let Some(used) = parse_header(&response.headers, "x-mbx-order-count-1d") {
        feedback.usage.push(RateLimitUsage {
            rate_limit_type: RateLimitType::Orders,
            interval_nanos: Duration::from_secs(24 * 60 * 60).as_nanos(),
            used: Some(used),
            limit: None,
        });
    }
    feedback
}

fn parse_header(headers: &[(String, String)], name: &str) -> Option<u32> {
    headers
        .iter()
        .find(|(header_name, _)| header_name == name)
        .and_then(|(_, value)| value.trim().parse().ok())
}

fn synchronization(timeout: Duration) -> Synchronization<BinanceHttpRequest, BinanceHttpResponse> {
    let create_time_request = || BinanceHttpRequest {
        unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
        signature: None,
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

fn request_weight(request: &BinanceHttpRequest) -> u32 {
    // Binance's documented request weights live on the unsigned request
    // type in exchange-types, so the gateway has no binance-specific
    // rate-limit knowledge of its own.
    request.unsigned.weight()
}

fn order_count(request: &BinanceHttpRequest) -> u32 {
    request.unsigned.order_count()
}

#[cfg(test)]
mod test {
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::rate_limit::{
        rate_limit_config::RateLimitConfig, rate_limit_type::RateLimitType,
        rate_limiter::RateLimiter, rate_limits::RateLimits,
    };

    use super::*;
    use exchange_types::{
        binance::{
            asset_limits::BinanceAssetLimitsParams,
            error::BinanceError,
            exchange_info::{
                BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
                BinanceExchangeInfoResult, BinanceExchangeInfoSymbolStatus, BinanceOrderType,
            },
            rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
            signature::BinanceSignature,
            spot::{
                BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
                BinanceSpotOrderParams, BinanceTimeInForce,
            },
            time::BinanceTimeResult,
        },
        http::HttpMethod,
    };

    fn spot_order_params() -> BinanceSpotOrderParams {
        BinanceSpotOrderParams {
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
    fn signature() -> BinanceSignature {
        BinanceSignature {
            apiKey: "my-api-key".into(),
            signature: "signature".into(),
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
    fn spot_order_request_carries_the_signature_api_key_header() {
        // The X-MBX-APIKEY header comes from the request's signature, which
        // carries the api key (the params themselves carry none).
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params())),
            signature: Some(signature()),
        };
        let request = to_request(request).unwrap();
        assert!(matches!(request.method, HttpMethod::POST));
        assert!(
            request
                .headers
                .contains(&("X-MBX-APIKEY".into(), "my-api-key".into())),
            "headers: {:?}",
            request.headers
        );
        let query = request.query.as_deref().expect("a query");
        assert!(query.starts_with("order?"), "query: {query}");
        assert!(query.ends_with("&signature=signature"), "query: {query}");
    }

    #[test]
    fn unsigned_request_omits_the_api_key_header() {
        // Requests without a signature carry no api key: no header and no
        // `signature` query parameter are added.
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params())),
            signature: None,
        };
        let request = to_request(request).unwrap();
        assert!(
            !request
                .headers
                .iter()
                .any(|(name, _)| name == "X-MBX-APIKEY"),
            "headers: {:?}",
            request.headers
        );
        let query = request.query.as_deref().expect("a query");
        assert!(!query.contains("apiKey"), "query: {query}");
        assert!(!query.contains("signature"), "query: {query}");
    }

    #[test]
    fn asset_limits_request_carries_the_signature_api_key_header() {
        // `/api/v3/myFilters` is a USER_DATA endpoint: like any signed
        // request it carries the X-MBX-APIKEY header from the signature and
        // the signature query parameter on the endpoint's origin form.
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
                recvWindow: None,
                symbol: "BNBUSDT".into(),
                timestamp: 1700000000000,
            }),
            signature: Some(signature()),
        };
        let request = to_request(request).unwrap();
        assert!(matches!(request.method, HttpMethod::GET));
        assert!(
            request
                .headers
                .contains(&("X-MBX-APIKEY".into(), "my-api-key".into())),
            "headers: {:?}",
            request.headers
        );
        assert_eq!(
            request.query.as_deref(),
            Some("myFilters?symbol=BNBUSDT&timestamp=1700000000000&signature=signature")
        );
    }

    #[test]
    fn exchange_info_query_is_forwarded() {
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
            signature: None,
        };
        let request = to_request(request).unwrap();
        assert!(matches!(request.method, HttpMethod::GET));
        assert_eq!(
            request.query.as_deref(),
            Some("exchangeInfo?permissions=SPOT&symbolStatus=TRADING")
        );
    }

    #[test]
    fn exchange_info_omits_empty_permissions() {
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
            signature: None,
        };
        let request = to_request(request).unwrap();
        assert_eq!(
            request.query.as_deref(),
            Some("exchangeInfo?symbolStatus=TRADING")
        );
    }

    #[test]
    fn time_request_is_unsigned_and_routed_to_the_time_endpoint() {
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
            signature: None,
        };
        let transport_request = to_request(request).unwrap();
        assert!(matches!(transport_request.method, HttpMethod::GET));
        assert!(transport_request.headers.is_empty());
        assert_eq!(transport_request.query.as_deref(), Some("time?"));
    }

    #[test]
    fn amend_order_request_is_a_put_to_the_cancel_replace_endpoint() {
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::AmendOrderRequest(
                exchange_types::binance::amend::BinanceAmendOrderParams {
                    newClientOrderId: Some("abc".into()),
                    newQty: "1".parse().unwrap(),
                    orderId: Some(123),
                    origClientOrderId: None,
                    recvWindow: None,
                    symbol: "BTCUSDT".into(),
                    timestamp: 1700000000000,
                },
            ),
            signature: Some(signature()),
        };
        let request = to_request(request).unwrap();
        assert!(matches!(request.method, HttpMethod::PUT));
        let query = request.query.as_deref().expect("a query");
        assert!(query.starts_with("order/cancelReplace?"), "query: {query}");
        assert!(query.ends_with("&signature=signature"), "query: {query}");
    }

    #[test]
    fn header_feedback_parses_binance_usage_headers() {
        let response = HttpResponse {
            status: 200,
            body: vec![],
            headers: vec![
                ("x-mbx-used-weight-1m".into(), "1200".into()),
                ("x-mbx-order-count-10s".into(), "3".into()),
                ("x-mbx-order-count-1d".into(), "12".into()),
            ],
        };
        let feedback = http_header_feedback(&response);
        assert_eq!(feedback.usage.len(), 3);
        assert_eq!(
            feedback.usage[0].rate_limit_type,
            RateLimitType::RequestWeight
        );
        assert_eq!(
            feedback.usage[0].interval_nanos,
            Duration::from_secs(60).as_nanos()
        );
        assert_eq!(feedback.usage[0].used, Some(1200));
        assert_eq!(feedback.usage[1].rate_limit_type, RateLimitType::Orders);
        assert_eq!(
            feedback.usage[1].interval_nanos,
            Duration::from_secs(10).as_nanos()
        );
        assert_eq!(feedback.usage[1].used, Some(3));
        assert_eq!(
            feedback.usage[2].interval_nanos,
            Duration::from_secs(24 * 60 * 60).as_nanos()
        );
        assert_eq!(feedback.usage[2].used, Some(12));
    }

    #[test]
    fn header_feedback_ignores_missing_headers() {
        let response = HttpResponse {
            status: 200,
            body: vec![],
            headers: vec![],
        };
        assert!(http_header_feedback(&response).usage.is_empty());
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
    fn request_weights_match_binance_docs() {
        let exchange_info = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
            signature: None,
        };
        assert_eq!(request_weight(&exchange_info), 20);
        let asset_limits = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
                recvWindow: None,
                symbol: "BNBUSDT".into(),
                timestamp: 0,
            }),
            signature: None,
        };
        assert_eq!(request_weight(&asset_limits), 40);
        let order = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot_order_params())),
            signature: None,
        };
        assert_eq!(request_weight(&order), 1);
        assert_eq!(order_count(&order), 1);
        let time = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
            signature: None,
        };
        assert_eq!(request_weight(&time), 1);
        assert_eq!(order_count(&time), 0);
    }

    #[test]
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
    fn sync_clock_syncs_the_server_clock() {
        let clock = Clock::default();
        let synchronization = synchronization(Duration::from_secs(20));
        let message = (synchronization.create_time_request)();
        assert!(matches!(
            message.unsigned,
            BinanceHttpUnsignedRequest::Time(..)
        ));
        assert!((to_filter(&message))(&BinanceHttpResponse::Success(
            BinanceHttpResponseResult::Time(BinanceTimeResult {
                serverTime: 1700000000000
            })
        )));
        let local = clock.now_millis();
        let response =
            BinanceHttpResponse::Success(BinanceHttpResponseResult::Time(BinanceTimeResult {
                serverTime: local + 10_000,
            }));
        let server_time =
            (synchronization.to_server_time)(&response).expect("No server time from response");
        clock
            .sync(server_time, Duration::ZERO)
            .expect("Cannot sync clock");
        assert!(
            clock.now_millis() >= local + 10_000,
            "now: {}",
            clock.now_millis()
        );
    }

    #[test]
    fn sync_clock_surfaces_the_time_error() {
        let synchronization = synchronization(Duration::from_secs(20));
        let response = BinanceHttpResponse::Failure(BinanceError {
            code: -1021,
            msg: "Timestamp for this request is outside of the recvWindow.".into(),
        });
        let result = (synchronization.to_server_time)(&response);
        assert!(result.is_err(), "expected ApiError");
        let Err(EGError::ApiError { code, message }) = result else {
            panic!("expected an ApiError");
        };
        assert_eq!(code, -1021);
        assert!(message.contains("recvWindow"));
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
    /// with a fixed outcome, so post-send failure budget behaviour can be
    /// tested without a network.
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
            message: Self::TransportReq,
            _timeout: Duration,
        ) -> EGResult<Self::TransportRes> {
            self.sent.lock().unwrap().push(message);
            match self.outcome {
                ScriptedOutcome::RateLimited => Err(EGError::RateLimited(RateLimitFeedback {
                    is_throttled: false,
                    retry_after: None,
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
    /// failed sync_clock can be observed.
    fn scripted_http_connector(
        client_handle: std::sync::mpsc::Sender<ScriptedHttpClient>,
        outcome: ScriptedOutcome,
        rate_limits: RateLimits,
    ) -> EGResult<impl Connector<BinanceHttpRequest, BinanceHttpResponse>> {
        let scripted_client = ScriptedHttpClient {
            sent: Arc::new(Mutex::new(Vec::new())),
            outcome,
        };
        let _ = client_handle.send(scripted_client.clone());
        let client: Arc<
            dyn HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse>,
        > = Arc::new(scripted_client);
        let transport = HttpTransport::new(
            client,
            Arc::new(to_request),
            from_response,
            rate_limits.clone(),
            |_: &HttpResponse| RateLimitFeedback::default(),
            response_feedback,
        );
        Ok(ConnectorImpl::new(
            rate_limits,
            Clock::default(),
            synchronization(Duration::from_secs(20)),
            request_weight,
            order_count,
            to_filter,
            Transport::Http(transport),
        ))
    }

    /// A one-slot budget for both weight and orders: a single consumed
    /// request exhausts the budget until it is refunded.
    fn single_slot_rate_limits() -> RateLimits {
        RateLimits {
            weight: RateLimiter::new(vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }]),
            orders: RateLimiter::new(vec![RateLimitConfig {
                rate_limit_type: RateLimitType::Orders,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(10).as_nanos(),
            }]),
        }
    }

    #[tokio::test]
    async fn sync_clock_keeps_local_reservation_on_business_rejection() {
        let (client_tx, client_rx) = std::sync::mpsc::channel();
        let connector = scripted_http_connector(
            client_tx,
            ScriptedOutcome::HttpError,
            single_slot_rate_limits(),
        )
        .unwrap();
        let client = client_rx.recv().unwrap();

        // The time request is rejected with a 4xx business error, but
        // Binance counts its weight anyway: the locally-reserved capacity
        // must not be refunded.
        let result = connector.sync_clock().await;
        assert!(matches!(
            result,
            Err(EGError::HttpError { status: 400, .. })
        ));

        // The budget stays exhausted, so a second sync_clock is rejected by
        // the local limiter and never reaches the transport.
        let result = connector.sync_clock().await;
        assert!(matches!(result, Err(EGError::RateLimited { .. })));
        assert_eq!(client.sent.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn sync_clock_refunds_local_reservation_on_rate_limited() {
        let (client_tx, client_rx) = std::sync::mpsc::channel();
        let connector = scripted_http_connector(
            client_tx,
            ScriptedOutcome::RateLimited,
            single_slot_rate_limits(),
        )
        .unwrap();
        let client = client_rx.recv().unwrap();

        // A server-side 429 is not counted against the request-weight
        // budget, so the locally-reserved capacity is refunded.
        let result = connector.sync_clock().await;
        assert!(matches!(result, Err(EGError::RateLimited { .. })));
        assert_eq!(client.sent.lock().unwrap().len(), 1);

        // The refunded budget admits the next sync_clock: it reaches the
        // transport again instead of being rejected by the local limiter.
        let result = connector.sync_clock().await;
        assert!(matches!(result, Err(EGError::RateLimited { .. })));
        assert_eq!(client.sent.lock().unwrap().len(), 2);
    }
}
