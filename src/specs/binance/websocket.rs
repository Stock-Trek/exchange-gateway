use crate::{
    auth_gate::AuthGate,
    authenticate_leg::AuthenticateLeg,
    clock::{Clock, Synchronization},
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue, BoxTryCreateOnce, TryConvertValue},
    listeners::{
        convert_listener::ConvertListener, listener::ListenerTrait,
        websocket_listener::WebsocketListener,
    },
    rate_limit::feedback::RateLimitFeedback,
    sign::{
        convert_signer::ConvertSigner, into_signed::IntoSigned, message_signer::MessageSigner,
        signer::Signer,
    },
    specs::binance::common::{
        exchange_urls, id, rate_limit_usage, rate_limits, signer, sync_timestamp_fields,
    },
    transports::{
        transport::Transport,
        websocket::{WebsocketClientTrait, WebsocketTransport},
    },
    urls::{ExchangeTransportType, TradingMode},
};
use exchange_types::binance::{
    logon::BinanceLogonParams,
    time::BinanceTimeParams,
    websocket::{
        BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketResponseResult,
        BinanceWebsocketSignedParams, BinanceWebsocketUnsignedParams,
        BinanceWebsocketUnsignedRequest,
    },
};
use std::{sync::Arc, time::Duration};

pub(crate) fn connector<ExternalReq, ExternalRes>(
    trading_mode: TradingMode,
    to_unsigned_request: TryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
    credentials: Option<ApiKeyCredentials>,
    clock: Clock,
    listener: impl ListenerTrait<TMessage = ExternalRes> + 'static,
    use_session: bool,
    logon_timeout: Duration,
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
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    ExternalReq: Send + Sync,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let url = exchange_urls().url(ExchangeTransportType::Websocket, trading_mode);
    let rate_limits = rate_limits();
    let response_listener = ConvertListener::new(to_external_response, listener);
    let auth_gate = Arc::new(AuthGate::default());
    let websocket_listener = Arc::new(WebsocketListener::new(
        Arc::new(from_response),
        response_feedback,
        rate_limits.clone(),
        response_listener,
        auth_gate.clone(),
    ));
    let client = Arc::new(client_creator((url, websocket_listener.clone()))?);
    let transport = WebsocketTransport::new(
        client,
        to_request,
        Arc::new(from_response),
        websocket_listener,
    );
    let authenticate_legs = if use_session {
        if credentials.is_none() {
            return Err(EGError::NotAuthenticated);
        }
        vec![authenticate_leg(logon_timeout)]
    } else {
        vec![]
    };
    Ok(ConnectorImpl::new(
        rate_limits,
        clock,
        synchronization(Duration::from_secs(20)),
        to_unsigned_request,
        request_weight,
        order_count,
        sync_timestamp(),
        to_filter,
        send_to_external_response(to_external_response),
        Transport::Websocket(transport),
        null_signer(),
        credentials,
        create_signer_from_credentials,
        authenticate_legs,
        auth_gate,
    ))
}

fn to_filter(
    mut request: BinanceWebsocketUnsignedRequest,
) -> (
    BinanceWebsocketUnsignedRequest,
    ArcPredicate<BinanceWebsocketResponse>,
) {
    let request_id = id();
    request.id = request_id.clone();
    let filter: ArcPredicate<BinanceWebsocketResponse> =
        Arc::new(move |response: &BinanceWebsocketResponse| response.id == request_id);
    (request, filter)
}

fn send_to_external_response<ExternalRes>(
    to_external_response: TryConvertValue<BinanceWebsocketResponse, ExternalRes>,
) -> ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>
where
    ExternalRes: Send + Sync + 'static,
{
    Arc::new(move |response| {
        validate_response(&response)?;
        to_external_response(response)
    })
}

fn to_request(request: BinanceWebsocketRequest) -> EGResult<BinanceWebsocketRequest> {
    Ok(request)
}

fn from_response(response: BinanceWebsocketResponse) -> EGResult<BinanceWebsocketResponse> {
    Ok(response)
}

fn authenticate_leg(
    timeout: Duration,
) -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let create_auth_attempt = Arc::new(|clock: &Clock| {
        let id = id();
        let message = auth_message(&id, clock);
        let filter: ArcPredicate<BinanceWebsocketResponse> =
            Arc::new(move |response: &BinanceWebsocketResponse| response.id == id);
        (message, filter)
    });
    let create_signer = {
        move |message| -> EGResult<
            Option<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>>,
        > {
            validate_response(&message)?;
            Ok(Some(Box::new(ConvertSigner::new(converter))))
        }
    };
    AuthenticateLeg {
        create_auth_attempt,
        create_signer,
        timeout,
    }
}

fn auth_message(id: &str, clock: &Clock) -> BinanceWebsocketUnsignedRequest {
    let timestamp = clock.now_millis();
    let params = BinanceLogonParams { timestamp };
    BinanceWebsocketUnsignedRequest {
        id: id.to_string(),
        params: BinanceWebsocketUnsignedParams::Logon(params),
    }
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
) -> Synchronization<BinanceWebsocketUnsignedRequest, BinanceWebsocketResponse> {
    let create_time_request = move || {
        let id = id();
        let message = BinanceWebsocketUnsignedRequest {
            id: id.to_string(),
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
        };
        let filter: ArcPredicate<BinanceWebsocketResponse> =
            Arc::new(move |response: &BinanceWebsocketResponse| response.id == id);
        (message, filter)
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

fn null_signer() -> ConvertSigner<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest> {
    ConvertSigner::new(|unsigned| {
        let BinanceWebsocketUnsignedRequest { id, params } = unsigned;
        Ok(BinanceWebsocketRequest {
            id,
            params: BinanceWebsocketSignedParams {
                unsigned: params,
                signature: None,
            },
        })
    })
}

fn sync_timestamp()
-> TryConvertValue<(BinanceWebsocketUnsignedRequest, i64), BinanceWebsocketUnsignedRequest> {
    move |(mut request, server_time)| {
        match &mut request.params {
            BinanceWebsocketUnsignedParams::AmendOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
            }
            BinanceWebsocketUnsignedParams::AssetLimits(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
            }
            BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
            }
            BinanceWebsocketUnsignedParams::CancelOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
            }
            BinanceWebsocketUnsignedParams::Logon(params) => {
                params.timestamp = server_time;
            }
            BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, server_time);
            }
            BinanceWebsocketUnsignedParams::ExchangeInfo(..)
            | BinanceWebsocketUnsignedParams::Time(..) => {}
        }
        Ok(request)
    }
}

fn create_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    Ok(Box::new(MessageSigner::new(signer(credentials)?)))
}

impl IntoSigned for BinanceWebsocketUnsignedRequest {
    type Signed = BinanceWebsocketRequest;
    fn into_signed(self, signer: &exchange_types::signer::Signer) -> EGResult<Self::Signed> {
        self.into_signed(signer)
            .map_err(|error| EGError::External(Box::new(error)))
    }
}

fn converter(unsigned: BinanceWebsocketUnsignedRequest) -> EGResult<BinanceWebsocketRequest> {
    let BinanceWebsocketUnsignedRequest { id, params } = unsigned;
    Ok(BinanceWebsocketRequest {
        id,
        params: BinanceWebsocketSignedParams {
            unsigned: params,
            signature: None,
        },
    })
}

fn response_feedback(response: &BinanceWebsocketResponse) -> EGResult<RateLimitFeedback> {
    let mut feedback = RateLimitFeedback::default();
    feedback
        .usage
        .extend(response.rateLimits.iter().filter_map(rate_limit_usage));
    Ok(feedback)
}

fn request_weight(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
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

fn order_count(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(..) => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{clock::Clock, rate_limit::rate_limit_type::RateLimitType};
    use exchange_types::binance::time::BinanceTimeResult;
    use exchange_types::binance::{
        amend::BinanceAmendOrderParams,
        cancel::{BinanceCancelAllOrdersParams, BinanceCancelOrderParams},
        error::BinanceError,
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus, BinanceOrderType,
        },
        rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
    };
    use rust_decimal::Decimal;
    use secrecy::SecretString;
    use std::{sync::Arc, time::Duration};

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
    fn logon_filter_matches_any_response_for_the_logon_id() {
        let leg = authenticate_leg(Duration::from_secs(20));
        let clock = Arc::new(Clock::default());
        let (message, filter) = (leg.create_auth_attempt)(&clock);
        let id = message.id;
        // Success and rejection are both matched, so a rejected logon is
        // consumed by the authentication waiter instead of leaking to the
        // user's listener. The rejection itself is surfaced by `create_signer`.
        assert!(filter(&logon_response(id.clone(), 200, None)));
        assert!(filter(&logon_response(
            id.clone(),
            200,
            Some(BinanceError {
                code: -2014,
                msg: "API-key format invalid.".into(),
            }),
        )));
        assert!(filter(&logon_response(id.clone(), 401, None)));
        // Responses for other requests do not match.
        assert!(!filter(&logon_response("some-other-id".into(), 200, None)));
    }

    #[test]
    fn each_authentication_attempt_uses_a_fresh_logon_id() {
        let leg = authenticate_leg(Duration::from_secs(20));
        let clock = Arc::new(Clock::default());
        // A retried authentication must not reuse the previous attempt's id:
        // a slow response to the earlier attempt (e.g. one arriving after a
        // reconnect) would otherwise resolve the newer attempt's waiter, and
        // the newer attempt's own response would leak to the user's listener
        // with no waiter left.
        let (first_message, first_filter) = (leg.create_auth_attempt)(&clock);
        let (second_message, second_filter) = (leg.create_auth_attempt)(&clock);
        assert_ne!(
            first_message.id, second_message.id,
            "each authentication attempt must use a fresh logon id"
        );
        // Each attempt's waiter matches only that attempt's response.
        assert!(first_filter(&logon_response(
            first_message.id.clone(),
            200,
            None
        )));
        assert!(!first_filter(&logon_response(
            second_message.id.clone(),
            200,
            None
        )));
        assert!(second_filter(&logon_response(
            second_message.id.clone(),
            200,
            None
        )));
        assert!(!second_filter(&logon_response(
            first_message.id.clone(),
            200,
            None
        )));
    }

    #[test]
    fn logon_signer_surfaces_rejected_logon_error() {
        let leg = authenticate_leg(Duration::from_secs(20));
        let clock = Arc::new(Clock::default());
        let id = (leg.create_auth_attempt)(&clock).0.id;
        // A successful logon response yields a signer.
        let successful_response = logon_response(id.clone(), 200, None);
        assert!((leg.create_signer)(successful_response).is_ok());
        // A rejected logon surfaces the exchange's actual error.
        let error_response = logon_response(
            id.clone(),
            401,
            Some(BinanceError {
                code: -2014,
                msg: "API-key format invalid.".into(),
            }),
        );
        match (leg.create_signer)(error_response) {
            Err(EGError::ApiError { code, message }) => {
                assert_eq!(code, -2014);
                assert_eq!(message, "API-key format invalid.");
            }
            _ => panic!("expected ApiError"),
        }
        // A non-200 status without an error object is also a rejection.
        let unsuccessful_response_without_error = logon_response(id.clone(), 503, None);
        assert!(matches!(
            (leg.create_signer)(unsuccessful_response_without_error),
            Err(EGError::ApiError { .. })
        ));
    }

    #[test]
    fn converter_omits_signature_after_session_logon() {
        // After session.logon the WebSocket API authenticates the connection:
        // post-logon requests carry no signature.
        let unsigned = BinanceWebsocketUnsignedRequest {
            id: "1".into(),
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let signed = converter(unsigned).unwrap();
        assert!(signed.params.signature.is_none());
        assert!(matches!(
            signed.params.unsigned,
            BinanceWebsocketUnsignedParams::SpotOrderRequest(..)
        ));
    }

    #[test]
    fn converter_omits_signature_from_every_signed_request_type() {
        let params = vec![
            BinanceWebsocketUnsignedParams::AmendOrderRequest(BinanceAmendOrderParams {
                newClientOrderId: None,
                newQty: Decimal::from(1),
                orderId: Some(1),
                origClientOrderId: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 0,
            }),
            BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(BinanceCancelAllOrdersParams {
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 0,
            }),
            BinanceWebsocketUnsignedParams::CancelOrderRequest(BinanceCancelOrderParams {
                cancelRestrictions: None,
                newClientOrderId: None,
                orderId: Some(1),
                origClientOrderId: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 0,
            }),
            BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        ];
        for (index, params) in params.into_iter().enumerate() {
            let unsigned = BinanceWebsocketUnsignedRequest {
                id: index.to_string(),
                params,
            };
            let signed = converter(unsigned).unwrap();
            assert!(signed.params.signature.is_none());
        }
    }

    #[test]
    fn signed_request_carries_api_key_and_signature() {
        // The WebSocket API signs all params except signature and sends the
        // apiKey in the signed payload; the exchange-types signer does this
        // internally.
        let credentials = ApiKeyCredentials {
            api_key: "my-api-key".into(),
            secret: SecretString::from("my-secret"),
        };
        let signer = create_signer_from_credentials(&credentials).unwrap();
        let request = BinanceWebsocketUnsignedRequest {
            id: "1".into(),
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let signed = signer.sign(request).unwrap();
        let signature = signed
            .params
            .signature
            .expect("signed request must carry a signature");
        assert_eq!(signature.apiKey, "my-api-key");
        assert!(!signature.signature.is_empty());
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
    fn sync_timestamp_fills_fresh_timestamp_and_default_recv_window() {
        let clock = Arc::new(Clock::default());
        let before = clock.now_millis();
        let sync = sync_timestamp();
        let request = BinanceWebsocketUnsignedRequest {
            id: "1".into(),
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let synced = sync((request, clock.now_millis())).unwrap();
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
    fn request_weights_match_binance_docs() {
        let logon = BinanceWebsocketUnsignedRequest {
            id: "1".into(),
            params: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams { timestamp: 0 }),
        };
        assert_eq!(request_weight(&logon), 2);
        let exchange_info = BinanceWebsocketUnsignedRequest {
            id: "2".into(),
            params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
        };
        assert_eq!(request_weight(&exchange_info), 20);
        let time = BinanceWebsocketUnsignedRequest {
            id: "3".into(),
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
        };
        assert_eq!(request_weight(&time), 1);
    }

    #[test]
    fn sync_lock_syncs_the_server_clock() {
        let clock = Arc::new(Clock::default());
        let synchronization = synchronization(Duration::from_secs(20));
        let (message, filter) = (synchronization.create_time_request)();
        assert!(matches!(
            message.params,
            BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {})
        ));
        // The sync clock is unsigned: the request carries no payload to sign
        // (the transport sends it without an API key).
        let id = message.id;
        let local = clock.now_millis();
        let response = BinanceWebsocketResponse {
            error: None,
            id,
            rateLimits: vec![],
            result: Some(BinanceWebsocketResponseResult::Time(BinanceTimeResult {
                serverTime: local + 10_000,
            })),
            status: 200,
        };
        assert!(filter(&response));
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
    fn sync_timestamp_leaves_time_unchanged() {
        let clock = Arc::new(Clock::default());
        let sync = sync_timestamp();
        let request = BinanceWebsocketUnsignedRequest {
            id: "1".into(),
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
        };
        let synced = sync((request, clock.now_millis())).unwrap();
        assert!(matches!(
            synced.params,
            BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {})
        ));
    }
}
