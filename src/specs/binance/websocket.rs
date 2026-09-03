use crate::{
    clock::{Clock, Synchronization},
    connector::Connector,
    connector_impl::ConnectorImpl,
    error::{EGError, EGResult},
    functions::{ArcPredicate, BoxTryCreateOnce},
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
    rate_limit::feedback::RateLimitFeedback,
    specs::binance::common::{exchange_urls, id, rate_limit_usage, rate_limits},
    transports::{
        transport::Transport,
        websocket::{WebsocketClientTrait, WebsocketTransport},
    },
    urls::{ExchangeTransportType, TradingMode},
};
use exchange_types::binance::{
    signed::BinanceSignedParams,
    time::BinanceTimeParams,
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketResponseResult, BinanceWebsocketUnsignedParams,
    },
};
use std::{sync::Arc, time::Duration};

pub(crate) fn connector(
    trading_mode: TradingMode,
    clock: Clock,
    listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
    client_creator: BoxTryCreateOnce<
        (
            String,
            Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
        ),
        impl WebsocketClientTrait<
            TransportReq = BinanceWebsocketRequest,
            TransportRes = BinanceWebsocketResponse,
        > + 'static,
    >,
) -> EGResult<impl Connector<BinanceWebsocketRequest, BinanceWebsocketResponse>> {
    let url = exchange_urls().url(ExchangeTransportType::Websocket, trading_mode);
    let rate_limits = rate_limits();
    let websocket_listener = Arc::new(WebsocketListener::new(
        Arc::new(from_response),
        response_feedback,
        rate_limits.clone(),
        listener,
    ));
    let client = Arc::new(client_creator((url, websocket_listener.clone()))?);
    let transport = WebsocketTransport::new(
        client,
        to_request,
        Arc::new(from_response),
        websocket_listener,
    );
    Ok(ConnectorImpl::new(
        rate_limits,
        clock,
        synchronization(Duration::from_secs(20)),
        request_weight,
        order_count,
        to_filter,
        Transport::Websocket(transport),
    ))
}

fn to_filter(request: &BinanceWebsocketRequest) -> ArcPredicate<BinanceWebsocketResponse> {
    let request_id = request.metadata.id.clone();
    Arc::new(move |response: &BinanceWebsocketResponse| response.id == request_id)
}

fn to_request(request: BinanceWebsocketRequest) -> EGResult<BinanceWebsocketRequest> {
    Ok(request)
}

fn from_response(response: BinanceWebsocketResponse) -> EGResult<BinanceWebsocketResponse> {
    Ok(response)
}

fn validate_response(response: &BinanceWebsocketResponse) -> EGResult<()> {
    if let Some(error) = &response.error {
        return Err(EGError::ApiError {
            code: error.code,
            message: error.msg.clone(),
        });
    }
    if !(200..300).contains(&response.status) {
        return Err(EGError::ApiError {
            code: response.status as i64,
            message: format!("Non 2xx result: {:?}", response.result),
        });
    }
    Ok(())
}

fn synchronization(
    timeout: Duration,
) -> Synchronization<BinanceWebsocketRequest, BinanceWebsocketResponse> {
    let create_time_request = || BinanceWebsocketRequest {
        metadata: BinanceWebsocketMetadata {
            id: id(),
            method: BinanceWebsocketMethodName::Time,
        },
        params: BinanceSignedParams {
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
            signature: None,
        },
    };
    let to_server_time = |response: &BinanceWebsocketResponse| -> EGResult<i64> {
        validate_response(response)?;
        match &response.result {
            Some(BinanceWebsocketResponseResult::Time(result)) => Ok(result.serverTime),
            _ => Err(EGError::BadResponse),
        }
    };
    Synchronization {
        create_time_request,
        timeout,
        to_server_time,
    }
}

fn response_feedback(response: &BinanceWebsocketResponse) -> EGResult<RateLimitFeedback> {
    let mut feedback = RateLimitFeedback::default();
    feedback
        .usage
        .extend(response.rateLimits.iter().filter_map(rate_limit_usage));
    Ok(feedback)
}

fn request_weight(request: &BinanceWebsocketRequest) -> u32 {
    match &request.params.params {
        BinanceWebsocketUnsignedParams::AmendOrderRequest(..) => 4,
        BinanceWebsocketUnsignedParams::AssetLimits(..) => 40,
        BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(..) => 1,
        BinanceWebsocketUnsignedParams::CancelOrderRequest(..) => 1,
        BinanceWebsocketUnsignedParams::ExchangeInfo(..) => 20,
        BinanceWebsocketUnsignedParams::Logon(..) => 2,
        BinanceWebsocketUnsignedParams::SpotOrderRequest(..) => 1,
        BinanceWebsocketUnsignedParams::Time(..) => 1,
    }
}

fn order_count(request: &BinanceWebsocketRequest) -> u32 {
    match &request.params.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(..) => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::rate_limit::rate_limit_type::RateLimitType;
    use exchange_types::binance::{
        error::BinanceError,
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus, BinanceOrderType,
        },
        logon::BinanceLogonParams,
        rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
        time::BinanceTimeResult,
    };
    use std::time::Duration;

    fn spot_order_request() -> BinanceWebsocketRequest {
        BinanceWebsocketRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceSignedParams {
                params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(
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
                    },
                )),
                signature: None,
            },
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

    #[test]
    fn filter_matches_the_request_id_only() {
        let request = spot_order_request();
        let filter = to_filter(&request);
        let id = request.metadata.id.clone();
        let response = BinanceWebsocketResponse {
            error: None,
            id: id.clone(),
            rateLimits: vec![],
            result: None,
            status: 200,
        };
        assert!(filter(&response));
        let other = BinanceWebsocketResponse {
            error: None,
            id: "some-other-id".into(),
            rateLimits: vec![],
            result: None,
            status: 200,
        };
        assert!(!filter(&other));
    }

    #[test]
    fn to_request_and_from_response_are_identity() {
        let request = spot_order_request();
        let round_tripped = to_request(request.clone()).unwrap();
        assert_eq!(round_tripped.metadata.id, request.metadata.id);
        let response = BinanceWebsocketResponse {
            error: None,
            id: request.metadata.id,
            rateLimits: vec![],
            result: None,
            status: 200,
        };
        let converted = from_response(response.clone()).unwrap();
        assert_eq!(converted.id, response.id);
    }

    #[test]
    fn feedback_reports_usage_on_every_response() {
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
        assert_eq!(feedback.usage[0].used, Some(2500));
        assert_eq!(feedback.usage[0].limit, Some(6000));
        assert_eq!(feedback.usage[1].rate_limit_type, RateLimitType::Orders);
        assert_eq!(
            feedback.usage[1].interval_nanos,
            Duration::from_secs(24 * 60 * 60).as_nanos()
        );
        assert_eq!(feedback.usage[1].used, Some(12));
        assert_eq!(feedback.usage[1].limit, Some(160_000));
    }

    #[test]
    fn request_weights_match_binance_docs() {
        let logon = BinanceWebsocketRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::Logon,
            },
            params: BinanceSignedParams {
                params: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams {
                    apiKey: "k".into(),
                    timestamp: 0,
                }),
                signature: None,
            },
        };
        assert_eq!(request_weight(&logon), 2);
        let exchange_info = BinanceWebsocketRequest {
            metadata: BinanceWebsocketMetadata {
                id: "2".into(),
                method: BinanceWebsocketMethodName::ExchangeInfo,
            },
            params: BinanceSignedParams {
                params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                    permissions: vec![BinanceExchangeInfoPermission::SPOT],
                    symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
                }),
                signature: None,
            },
        };
        assert_eq!(request_weight(&exchange_info), 20);
        assert_eq!(order_count(&exchange_info), 0);
        let time = BinanceWebsocketRequest {
            metadata: BinanceWebsocketMetadata {
                id: "3".into(),
                method: BinanceWebsocketMethodName::Time,
            },
            params: BinanceSignedParams {
                params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
                signature: None,
            },
        };
        assert_eq!(request_weight(&time), 1);
        let order = spot_order_request();
        assert_eq!(request_weight(&order), 1);
        assert_eq!(order_count(&order), 1);
    }

    #[test]
    fn sync_clock_syncs_the_server_clock() {
        let clock = Clock::default();
        let synchronization = synchronization(Duration::from_secs(20));
        let message = (synchronization.create_time_request)();
        assert!(matches!(
            message.params.params,
            BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {})
        ));
        // The time request carries no payload to sign.
        assert!(message.params.signature.is_none());
        let local = clock.now_millis();
        let response = BinanceWebsocketResponse {
            error: None,
            id: message.metadata.id.clone(),
            rateLimits: vec![],
            result: Some(BinanceWebsocketResponseResult::Time(BinanceTimeResult {
                serverTime: local + 10_000,
            })),
            status: 200,
        };
        assert!((to_filter(&message))(&response));
        let server_time = (synchronization.to_server_time)(&response).expect("No server time");
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
        let response = BinanceWebsocketResponse {
            error: Some(BinanceError {
                code: -2014,
                msg: "API-key format invalid.".into(),
            }),
            id: "1".into(),
            rateLimits: vec![],
            result: None,
            status: 200,
        };
        let result = (synchronization.to_server_time)(&response);
        let Err(EGError::ApiError { code, message }) = result else {
            panic!("expected an ApiError");
        };
        assert_eq!(code, -2014);
        assert_eq!(message, "API-key format invalid.");
    }
}
