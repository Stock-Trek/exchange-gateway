use crate::{
    auth_gate::AuthGate,
    authenticate_leg::AuthenticateLeg,
    clock::Clock,
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{ArcCombineValues, ArcPredicate, ArcTryConvertValue},
    listeners::{
        convert_listener::ConvertListener, listener::ListenerTrait,
        websocket_listener::WebsocketListener,
    },
    rate_limit::feedback::RateLimitFeedback,
    resync::Resync,
    sign::{
        convert_signer::ConvertSigner, encode::byte_encoding::ByteEncoding,
        message_signer::MessageSigner, signer::Signer,
    },
    specs::binance::common::{
        data_signer, exchange_urls, id, order_weight, rate_limit_usage, rate_limits,
        sync_timestamp_fields,
    },
    transports::{
        iris::IrisWebsocketClient, transport::Transport, websocket::WebsocketClientTrait,
        websocket::WebsocketTransport,
    },
    urls::{ExchangeTransportType, TradingMode},
};
use exchange_types::binance::{
    logon::BinanceLogonParams,
    signed::BinanceSignedParams,
    time::BinanceTimeParams,
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketResponseResult, BinanceWebsocketUnsignedParams,
        BinanceWebsocketUnsignedRequest,
    },
};
use iris::Config as IrisConfig;
use std::{sync::Arc, time::Duration};

pub(crate) fn connector<ExternalReq, ExternalRes>(
    trading_mode: TradingMode,
    to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
    listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    credentials: Option<ApiKeyCredentials>,
    use_session: bool,
    iris_config: IrisConfig,
    clock: Arc<Clock>,
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    ExternalReq: Send + Sync,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let url = exchange_urls().url(ExchangeTransportType::Websocket, trading_mode);
    let client_factory = move |websocket_listener: Arc<
        dyn ListenerTrait<TMessage = BinanceWebsocketResponse>,
    >|
          -> Arc<
        dyn WebsocketClientTrait<
                TransportReq = BinanceWebsocketRequest,
                TransportRes = BinanceWebsocketResponse,
            >,
    > {
        let client =
            IrisWebsocketClient::<BinanceWebsocketRequest, BinanceWebsocketResponse>::with_config(
                &url,
                iris_config,
                websocket_listener,
            );
        Arc::new(client)
    };
    connector_with_client_factory(
        client_factory,
        Duration::from_secs(20),
        to_unsigned_request,
        to_external_response,
        listener,
        credentials,
        use_session,
        clock,
    )
}

pub(crate) fn connector_with_client_factory<ExternalReq, ExternalRes>(
    client_factory: impl FnOnce(
        Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>>,
    ) -> Arc<
        dyn WebsocketClientTrait<
                TransportReq = BinanceWebsocketRequest,
                TransportRes = BinanceWebsocketResponse,
            >,
    >,
    logon_timeout: Duration,
    to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
    listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    credentials: Option<ApiKeyCredentials>,
    use_session: bool,
    clock: Arc<Clock>,
) -> EGResult<impl Connector<ExternalReq, ExternalRes>>
where
    ExternalReq: Send + Sync,
    ExternalRes: Clone + Send + Sync + 'static,
{
    let rate_limits = rate_limits();
    let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
        Arc::new(ConvertListener::new(to_external_response, listener));
    let auth_gate = Arc::new(AuthGate::default());
    let websocket_listener = Arc::new(WebsocketListener::new(
        Arc::new(from_response),
        response_feedback,
        rate_limits.clone(),
        response_listener,
        auth_gate.clone(),
    ));
    let client = client_factory(websocket_listener.clone());
    let transport = WebsocketTransport::new(
        client,
        Arc::new(to_request),
        Arc::new(from_response),
        websocket_listener,
    );
    let authenticate_legs = if use_session {
        let api_key = match &credentials {
            Some(credentials) => credentials.api_key.clone(),
            None => return Err(EGError::NotAuthenticated),
        };
        vec![
            time_bootstrap_leg(clock.clone(), Duration::from_secs(20)),
            authenticate_leg(api_key, clock.clone(), logon_timeout),
        ]
    } else {
        vec![]
    };
    Ok(ConnectorImpl::new(
        rate_limits,
        request_weight,
        order_count,
        to_unsigned_request,
        sync_timestamp(clock.clone()),
        Transport::Websocket(transport),
        null_signer(),
        credentials,
        create_signer_from_credentials,
        authenticate_legs,
        Some(time_resync(clock, Duration::from_secs(20))),
        auth_gate,
    ))
}

fn to_request(request: BinanceWebsocketRequest) -> EGResult<BinanceWebsocketRequest> {
    Ok(request)
}

fn from_response(response: BinanceWebsocketResponse) -> EGResult<BinanceWebsocketResponse> {
    Ok(response)
}

fn authenticate_leg(
    api_key: String,
    clock: Arc<Clock>,
    timeout: Duration,
) -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let create_auth_attempt = {
        let api_key = api_key.clone();
        let clock_auth = clock.clone();
        Arc::new(move || {
            let id = id();
            let message = auth_message(&id, &api_key, &clock_auth);
            let filter: ArcPredicate<BinanceWebsocketResponse> =
                Arc::new(move |response: &BinanceWebsocketResponse| response.id == id);
            (message, filter)
        })
    };
    let create_signer = {
        let clock_signer = clock.clone();
        Arc::new(
            move |(message, round_trip_time)| -> EGResult<
                Option<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>>,
            > {
                rejected_response_error(&message)?;
                sync_from_logon_response(&message, &clock_signer, round_trip_time)?;
                Ok(Some(Box::new(ConvertSigner::new(converter))))
            },
        )
    };
    AuthenticateLeg {
        create_auth_attempt,
        create_signer,
        timeout,
    }
}

fn auth_message(id: &str, api_key: &str, clock: &Clock) -> BinanceWebsocketUnsignedRequest {
    let timestamp = clock.now_millis();
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
    clock: &Clock,
    round_trip_time: Duration,
) -> EGResult<()> {
    if let Some(BinanceWebsocketResponseResult::SessionAuthentication(result)) = &message.result {
        clock.sync(result.serverTime, round_trip_time);
    }
    Ok(())
}

fn rejected_response_error(message: &BinanceWebsocketResponse) -> EGResult<()> {
    if let Some(error) = &message.error {
        return Err(EGError::ApiError {
            code: error.code,
            message: error.msg.clone(),
        });
    }
    if message.status != 200 {
        return Err(EGError::ApiError {
            code: message.status as i64,
            message: format!("Response rejected with status {}", message.status),
        });
    }
    Ok(())
}

pub(crate) fn time_bootstrap_leg(
    clock: Arc<Clock>,
    timeout: Duration,
) -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let resync = time_resync(clock, timeout);
    let create_signer = {
        let sync_clock = resync.sync_clock.clone();
        Arc::new(
            move |(message, round_trip_time)| -> EGResult<
                Option<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>>,
            > {
                (sync_clock)((message, round_trip_time))?;
                Ok(None)
            },
        )
    };
    AuthenticateLeg {
        create_auth_attempt: resync.create_request,
        timeout: resync.timeout,
        create_signer,
    }
}

/// The capability to re-sync the server clock from the unsigned WebSocket
/// `time` request: a fresh request (with its own id) whose response re-adopts
/// the server's offset, so timestamps are stamped with the server clock even
/// when the local clock is skewed beyond the recvWindow.
fn time_resync(
    clock: Arc<Clock>,
    timeout: Duration,
) -> Resync<BinanceWebsocketUnsignedRequest, BinanceWebsocketResponse> {
    let create_request = Arc::new(move || {
        let id = id();
        let message = time_bootstrap_message(&id);
        let filter: ArcPredicate<BinanceWebsocketResponse> =
            Arc::new(move |response: &BinanceWebsocketResponse| response.id == id);
        (message, filter)
    });
    let sync_clock = {
        let clock = clock.clone();
        Arc::new(
            move |(message, round_trip_time): (BinanceWebsocketResponse, Duration)| -> EGResult<()> {
                rejected_response_error(&message)?;
                sync_from_time_response(&message, &clock, round_trip_time)
            },
        )
    };
    Resync {
        create_request,
        timeout,
        sync_clock,
    }
}

fn time_bootstrap_message(id: &str) -> BinanceWebsocketUnsignedRequest {
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id: id.to_string(),
            method: BinanceWebsocketMethodName::Time,
        },
        params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
    }
}

fn sync_from_time_response(
    message: &BinanceWebsocketResponse,
    clock: &Clock,
    round_trip_time: Duration,
) -> EGResult<()> {
    if let Some(BinanceWebsocketResponseResult::Time(result)) = &message.result {
        clock.sync(result.serverTime, round_trip_time);
    }
    Ok(())
}

fn null_signer() -> ConvertSigner<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest> {
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

fn sync_timestamp(
    clock: Arc<Clock>,
) -> ArcTryConvertValue<BinanceWebsocketUnsignedRequest, BinanceWebsocketUnsignedRequest> {
    Arc::new(move |mut request| {
        match &mut request.params {
            BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &clock);
            }
            BinanceWebsocketUnsignedParams::AmendOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &clock);
            }
            BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &clock);
            }
            BinanceWebsocketUnsignedParams::CancelOrderRequest(params) => {
                sync_timestamp_fields(&mut params.timestamp, &mut params.recvWindow, &clock);
            }
            BinanceWebsocketUnsignedParams::Logon(params) => {
                params.timestamp = clock.now_millis();
            }
            BinanceWebsocketUnsignedParams::ExchangeInfo(..)
            | BinanceWebsocketUnsignedParams::Time(..) => {}
        }
        Ok(request)
    })
}

fn create_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { secret, .. } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        Arc::new(unsigned_request_params_to_bytes),
        data_signer(secret)?,
        ByteEncoding::HexLower,
        signature_appender(),
    )))
}

fn unsigned_request_params_to_bytes(
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
        BinanceWebsocketUnsignedParams::AmendOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::CancelOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::Time(..) => None,
    })
}

fn converter(unsigned: BinanceWebsocketUnsignedRequest) -> EGResult<BinanceWebsocketRequest> {
    let BinanceWebsocketUnsignedRequest { metadata, params } = unsigned;
    let params = BinanceSignedParams {
        signature: None,
        params: session_params(params),
    };
    Ok(BinanceWebsocketRequest { metadata, params })
}

/// After `session.logon` the WebSocket API authenticates the connection, so
/// post-logon requests must omit both `apiKey` and `signature`. Sending
/// `apiKey` without `signature` is an undocumented combination and is
/// rejected (-1022), so the session signer strips the `apiKey` the caller
/// placed on the request.
fn session_params(params: BinanceWebsocketUnsignedParams) -> BinanceWebsocketUnsignedParams {
    match params {
        BinanceWebsocketUnsignedParams::AmendOrderRequest(mut params) => {
            params.apiKey = None;
            BinanceWebsocketUnsignedParams::AmendOrderRequest(params)
        }
        BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(mut params) => {
            params.apiKey = None;
            BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(params)
        }
        BinanceWebsocketUnsignedParams::CancelOrderRequest(mut params) => {
            params.apiKey = None;
            BinanceWebsocketUnsignedParams::CancelOrderRequest(params)
        }
        BinanceWebsocketUnsignedParams::SpotOrderRequest(mut params) => {
            params.apiKey = None;
            BinanceWebsocketUnsignedParams::SpotOrderRequest(params)
        }
        // The `logon` (re-authentication goes through the full HMAC signer,
        // never the session signer) and the unsigned requests carry no
        // apiKey to strip.
        params @ (BinanceWebsocketUnsignedParams::Logon(..)
        | BinanceWebsocketUnsignedParams::ExchangeInfo(..)
        | BinanceWebsocketUnsignedParams::Time(..)) => params,
    }
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
        BinanceWebsocketUnsignedParams::ExchangeInfo(..) => 4,
        BinanceWebsocketUnsignedParams::Logon(..) => 2,
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => order_weight(params),
        BinanceWebsocketUnsignedParams::AmendOrderRequest(..) => 2,
        BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(..) => 1,
        BinanceWebsocketUnsignedParams::CancelOrderRequest(..) => 1,
        BinanceWebsocketUnsignedParams::Time(..) => 1,
    }
}

fn order_count(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(..) => 1,
        _ => 0,
    }
}

fn signature_appender()
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
        logon::BinanceSessionAuthenticationResult,
        rate_limits::{BinanceRateLimit, BinanceRateLimitInterval, BinanceRateLimitType},
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
    };
    use rust_decimal::Decimal;
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

    #[test]
    fn logon_filter_matches_any_response_for_the_logon_id() {
        let api_key = "api-key";
        let leg = authenticate_leg(
            api_key.into(),
            Arc::new(Clock::default()),
            Duration::from_secs(20),
        );
        let (message, filter) = (leg.create_auth_attempt)();
        let id = message.metadata.id;
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
        let api_key = "api-key";
        let leg = authenticate_leg(
            api_key.into(),
            Arc::new(Clock::default()),
            Duration::from_secs(20),
        );
        // A retried authentication must not reuse the previous attempt's id:
        // a slow response to the earlier attempt (e.g. one arriving after a
        // reconnect) would otherwise resolve the newer attempt's waiter, and
        // the newer attempt's own response would leak to the user's listener
        // with no waiter left.
        let (first_message, first_filter) = (leg.create_auth_attempt)();
        let (second_message, second_filter) = (leg.create_auth_attempt)();
        assert_ne!(
            first_message.metadata.id, second_message.metadata.id,
            "each authentication attempt must use a fresh logon id"
        );
        // Each attempt's waiter matches only that attempt's response.
        assert!(first_filter(&logon_response(
            first_message.metadata.id.clone(),
            200,
            None
        )));
        assert!(!first_filter(&logon_response(
            second_message.metadata.id.clone(),
            200,
            None
        )));
        assert!(second_filter(&logon_response(
            second_message.metadata.id.clone(),
            200,
            None
        )));
        assert!(!second_filter(&logon_response(
            first_message.metadata.id.clone(),
            200,
            None
        )));
    }

    #[test]
    fn logon_signer_surfaces_rejected_logon_error() {
        let api_key = "api-key";
        let leg = authenticate_leg(
            api_key.into(),
            Arc::new(Clock::default()),
            Duration::from_secs(20),
        );
        let id = (leg.create_auth_attempt)().0.metadata.id;
        // A successful logon response yields a signer.
        let successful_response = logon_response(id.clone(), 200, None);
        assert!((leg.create_signer)((successful_response, Duration::ZERO)).is_ok());
        // A rejected logon surfaces the exchange's actual error.
        let error_response = logon_response(
            id.clone(),
            401,
            Some(BinanceError {
                code: -2014,
                msg: "API-key format invalid.".into(),
            }),
        );
        match (leg.create_signer)((error_response, Duration::ZERO)) {
            Err(EGError::ApiError { code, message }) => {
                assert_eq!(code, -2014);
                assert_eq!(message, "API-key format invalid.");
            }
            _ => panic!("expected ApiError"),
        }
        // A non-200 status without an error object is also a rejection.
        let unsuccessful_response_without_error = logon_response(id.clone(), 503, None);
        assert!(matches!(
            (leg.create_signer)((unsuccessful_response_without_error, Duration::ZERO)),
            Err(EGError::ApiError { .. })
        ));
    }

    #[test]
    fn converter_omits_api_key_and_signature_after_session_logon() {
        // After session.logon the WebSocket API authenticates the connection:
        // post-logon requests must omit both apiKey and signature. Sending
        // apiKey without signature is an undocumented combination and is
        // rejected (-1022), so the session signer must strip the apiKey the
        // caller placed on the request.
        let unsigned = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let signed = converter(unsigned).unwrap();
        assert!(signed.params.signature.is_none());
        let BinanceWebsocketUnsignedParams::SpotOrderRequest(params) = signed.params.params else {
            panic!("expected a spot order request");
        };
        assert!(params.apiKey.is_none());
    }

    #[test]
    fn converter_strips_api_key_from_every_signed_request_type() {
        let params = vec![
            BinanceWebsocketUnsignedParams::AmendOrderRequest(BinanceAmendOrderParams {
                apiKey: Some("key".into()),
                newClientOrderId: None,
                newQty: Decimal::from(1),
                orderId: Some(1),
                origClientOrderId: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 0,
            }),
            BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(BinanceCancelAllOrdersParams {
                apiKey: Some("key".into()),
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 0,
            }),
            BinanceWebsocketUnsignedParams::CancelOrderRequest(BinanceCancelOrderParams {
                apiKey: Some("key".into()),
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
                metadata: BinanceWebsocketMetadata {
                    id: index.to_string(),
                    method: BinanceWebsocketMethodName::PlaceOrder,
                },
                params,
            };
            let signed = converter(unsigned).unwrap();
            assert!(signed.params.signature.is_none());
            let api_key = match &signed.params.params {
                BinanceWebsocketUnsignedParams::AmendOrderRequest(params) => &params.apiKey,
                BinanceWebsocketUnsignedParams::CancelAllOrdersRequest(params) => &params.apiKey,
                BinanceWebsocketUnsignedParams::CancelOrderRequest(params) => &params.apiKey,
                BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => &params.apiKey,
                _ => panic!("unexpected request type"),
            };
            assert!(
                api_key.is_none(),
                "post-logon {:?} must omit apiKey: {api_key:?}",
                signed.metadata.method
            );
        }
    }

    #[test]
    fn order_signature_payload_includes_api_key() {
        // The WebSocket API signs all params except signature, including
        // apiKey, sorted alphabetically.
        let request = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::PlaceOrder,
            },
            params: BinanceWebsocketUnsignedParams::SpotOrderRequest(Box::new(spot_order_params())),
        };
        let payload =
            String::from_utf8(unsigned_request_params_to_bytes(&request).unwrap().unwrap())
                .unwrap();
        assert!(payload.contains("apiKey=my-api-key"), "payload: {payload}");
        assert!(payload.contains("type=LIMIT"), "payload: {payload}");
        assert!(!payload.contains("r%23type"), "payload: {payload}");
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
        let sync = sync_timestamp(clock);
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
        let clock = Arc::new(Clock::default());
        let leg = authenticate_leg("api-key".into(), clock.clone(), Duration::from_secs(20));
        let local = clock.now_millis();
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
        let _signer = (leg.create_signer)((response, Duration::ZERO)).unwrap();
        assert!(
            clock.now_millis() >= local + 10_000,
            "now: {}",
            clock.now_millis()
        );
    }

    #[test]
    fn request_weights_match_binance_docs() {
        let logon = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::Logon,
            },
            params: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams {
                apiKey: "k".into(),
                timestamp: 0,
            }),
        };
        assert_eq!(request_weight(&logon), 2);
        let exchange_info = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "2".into(),
                method: BinanceWebsocketMethodName::ExchangeInfo,
            },
            params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
        };
        assert_eq!(request_weight(&exchange_info), 4);
        let time = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "3".into(),
                method: BinanceWebsocketMethodName::Time,
            },
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
        };
        assert_eq!(request_weight(&time), 1);
    }

    #[test]
    fn time_bootstrap_leg_syncs_the_server_clock() {
        let clock = Arc::new(Clock::default());
        let leg = time_bootstrap_leg(clock.clone(), Duration::from_secs(20));
        let (message, filter) = (leg.create_auth_attempt)();
        assert!(matches!(
            message.params,
            BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {})
        ));
        // The time bootstrap is unsigned: the request carries no payload to
        // sign (the transport sends it without an API key).
        assert!(matches!(
            message.params,
            BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {})
        ));
        let id = message.metadata.id;
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
        // The bootstrap records the server clock but installs no signer
        // (`Ok(None)` keeps the signer the previous leg installed).
        let signer = (leg.create_signer)((response, Duration::ZERO)).unwrap();
        assert!(signer.is_none());
        assert!(
            clock.now_millis() >= local + 10_000,
            "now: {}",
            clock.now_millis()
        );
    }

    #[test]
    fn sync_timestamp_leaves_time_unchanged() {
        let clock = Arc::new(Clock::default());
        let sync = sync_timestamp(clock);
        let request = BinanceWebsocketUnsignedRequest {
            metadata: BinanceWebsocketMetadata {
                id: "1".into(),
                method: BinanceWebsocketMethodName::Time,
            },
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
        };
        let synced = sync(request).unwrap();
        assert!(matches!(
            synced.params,
            BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {})
        ));
    }
}
