use crate::{
    clock::{Clock, Synchronization},
    connector::Connector,
    connector_impl::ConnectorImpl,
    error::{EGError, EGResult},
    functions::{ArcPredicate, BoxTryCreateOnce},
    rate_limiter::RateLimiter,
    transports::{
        http::{HttpClientTrait, HttpTransport},
        reqwest::{HttpRequest, HttpResponse},
        transport::Transport,
    },
};
use exchange_types::{
    binance::{
        http::{
            BinanceHttpRequest, BinanceHttpResponse, BinanceHttpResponseResult,
            BinanceHttpUnsignedRequest,
        },
        time::BinanceTimeParams,
        urls::BinanceUrls,
    },
    http_method::HttpMethod,
    rate_limited::RateLimited,
    urls::{Protocol, TradingMode, Urls},
};
use std::{sync::Arc, time::Duration};

pub(crate) fn connector(
    trading_mode: TradingMode,
    clock: Clock,
    rate_limiter: Arc<dyn RateLimiter>,
    client_creator: BoxTryCreateOnce<
        &str,
        impl HttpClientTrait<TransportReq = HttpRequest, TransportRes = HttpResponse> + 'static,
    >,
) -> EGResult<impl Connector<Request = BinanceHttpRequest, Response = BinanceHttpResponse>> {
    let url = BinanceUrls.url(Protocol::Http, trading_mode);
    let client = Arc::new(client_creator(url)?);
    let convert_request = Arc::new(to_request);
    let transport = HttpTransport::new(client, convert_request, from_response);
    Ok(ConnectorImpl::new(
        rate_limiter,
        clock,
        synchronization(Duration::from_secs(20)),
        to_weight,
        to_order_count,
        to_filter,
        Transport::Http(transport),
    ))
}

fn to_filter(_request: &BinanceHttpRequest) -> ArcPredicate<BinanceHttpResponse> {
    Arc::new(|_: &BinanceHttpResponse| true)
}

fn to_weight(request: &BinanceHttpRequest) -> u32 {
    request.unsigned.weight()
}

fn to_order_count(request: &BinanceHttpRequest) -> u32 {
    request.unsigned.order_count()
}

fn to_request(request: BinanceHttpRequest) -> EGResult<HttpRequest> {
    let endpoint = request.endpoint().to_string();
    let method = match request.http_method() {
        HttpMethod::DELETE => reqwest::Method::DELETE,
        HttpMethod::GET => reqwest::Method::GET,
        HttpMethod::POST => reqwest::Method::POST,
        HttpMethod::PUT => reqwest::Method::PUT,
    };
    let headers = request.headers();
    let query = Some(request.query_params());
    Ok(HttpRequest {
        endpoint,
        method,
        query,
        headers,
        body: None,
    })
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

#[cfg(test)]
mod test {
    use super::*;
    use exchange_types::binance::{
        amend::BinanceAmendOrderParams,
        asset_limits::BinanceAssetLimitsParams,
        cancel::{BinanceCancelAllOrdersParams, BinanceCancelOrderParams},
        error::BinanceError,
        exchange_info::BinanceExchangeInfoParams,
        exchange_info::{
            BinanceExchangeInfoPermission, BinanceExchangeInfoSymbolStatus, BinanceOrderType,
        },
        spot::{
            BinanceNewOrderResponseType, BinanceSelfTradeProtection, BinanceSide,
            BinanceSpotOrderParams, BinanceTimeInForce,
        },
        time::BinanceTimeResult,
    };
    use rust_decimal::Decimal;
    use std::time::Duration;

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
        assert_eq!(request.method, reqwest::Method::GET);
        assert_eq!(
            request.query.as_deref(),
            Some("permissions=SPOT&symbolStatus=TRADING")
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
        assert_eq!(request.query.as_deref(), Some("symbolStatus=TRADING"));
    }

    #[test]
    fn asset_limits_request_carries_the_api_key_header() {
        // `/api/v3/myFilters` is a USER_DATA endpoint: the signed query must
        // be accompanied by the X-MBX-APIKEY header taken from the
        // connector's credentials (the params carry no `apiKey` field).

        use exchange_types::binance::signature::BinanceSignature;
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::AssetLimits(BinanceAssetLimitsParams {
                recvWindow: None,
                symbol: "BNBUSDT".into(),
                timestamp: 1700000000000,
            }),
            signature: Some(BinanceSignature {
                apiKey: "my-api-key".into(),
                signature: "signature".into(),
            }),
        };
        let request = to_request(request).unwrap();
        assert_eq!(request.method, reqwest::Method::GET);
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
    fn signed_requests_carry_the_connector_api_key_header() {
        // Order/cancel/amend are USER_DATA endpoints: with credentials
        // configured the connector supplies the X-MBX-APIKEY header itself.
        let spot = spot_order_params();
        let requests = vec![
            BinanceHttpUnsignedRequest::SpotOrderRequest(Box::new(spot)),
            BinanceHttpUnsignedRequest::AmendOrderRequest(BinanceAmendOrderParams {
                newClientOrderId: None,
                newQty: Decimal::from(1),
                orderId: Some(1),
                origClientOrderId: None,
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 1700000000000,
            }),
            BinanceHttpUnsignedRequest::CancelAllOrdersRequest(BinanceCancelAllOrdersParams {
                recvWindow: None,
                symbol: "BTCUSDT".into(),
                timestamp: 1700000000000,
            }),
            BinanceHttpUnsignedRequest::CancelOrderRequest(BinanceCancelOrderParams {
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
            use exchange_types::binance::signature::BinanceSignature;

            let request = BinanceHttpRequest {
                unsigned: request,
                signature: Some(BinanceSignature {
                    apiKey: "connector-api-key".into(),
                    signature: "signature".into(),
                }),
            };
            let request = to_request(request).unwrap();
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
    fn time_request_is_unsigned_and_routed_to_the_time_endpoint() {
        let request = BinanceHttpRequest {
            unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
            signature: None,
        };
        // GET /api/v3/time with an empty query string and nothing to sign.
        let transport_request = to_request(request).unwrap();
        assert_eq!(transport_request.method, reqwest::Method::GET);
        assert_eq!(transport_request.query, Some("".into()));
        assert_eq!(
            BinanceHttpRequest {
                unsigned: BinanceHttpUnsignedRequest::Time(BinanceTimeParams {}),
                signature: None,
            }
            .endpoint(),
            "time"
        );
    }

    #[test]
    fn sync_clock_syncs_the_server_clock() {
        let clock = Arc::new(Clock::default());
        let synchronization = synchronization(Duration::from_secs(20));
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
}
