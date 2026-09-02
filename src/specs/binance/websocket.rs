use crate::{
    clock::{Clock, Synchronization},
    connector::Connector,
    connector_impl::ConnectorImpl,
    error::{EGError, EGResult},
    functions::{ArcPredicate, BoxTryCreateOnce},
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
    rate_limiter::RateLimiter,
    specs::binance::common::new_id,
    transports::{
        transport::Transport,
        websocket::{WebsocketClientTrait, WebsocketTransport},
    },
};
use exchange_types::{
    binance::{
        time::BinanceTimeParams,
        urls::BinanceUrls,
        websocket::{
            BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketResponseResult,
            BinanceWebsocketSignedParams, BinanceWebsocketUnsignedParams,
        },
    },
    rate_limited::RateLimited,
    urls::{Protocol, TradingMode, Urls},
};
use std::{sync::Arc, time::Duration};

pub(crate) fn connector(
    trading_mode: TradingMode,
    clock: Clock,
    rate_limiter: Arc<dyn RateLimiter>,
    listener: impl ListenerTrait<TMessage = BinanceWebsocketResponse> + 'static,
    client_creator: BoxTryCreateOnce<
        (
            &str,
            Arc<WebsocketListener<BinanceWebsocketResponse, BinanceWebsocketResponse>>,
        ),
        impl WebsocketClientTrait<
            TransportReq = BinanceWebsocketRequest,
            TransportRes = BinanceWebsocketResponse,
        > + 'static,
    >,
) -> EGResult<impl Connector<Request = BinanceWebsocketRequest, Response = BinanceWebsocketResponse>>
{
    let url = BinanceUrls.url(Protocol::Websocket, trading_mode);
    let websocket_listener = Arc::new(WebsocketListener::new(Arc::new(Ok), listener));
    let client = Arc::new(client_creator((url, websocket_listener.clone()))?);
    let transport = WebsocketTransport::new(client, Ok, Arc::new(Ok), websocket_listener);
    let connector = ConnectorImpl::new(
        rate_limiter,
        clock,
        synchronization(Duration::from_secs(20)),
        to_weight,
        to_order_count,
        to_filter,
        Transport::Websocket(transport),
    );
    Ok(connector)
}

fn to_filter(request: &BinanceWebsocketRequest) -> ArcPredicate<BinanceWebsocketResponse> {
    let id = request.id.clone();
    Arc::new(move |response: &BinanceWebsocketResponse| response.id == id)
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
        id: new_id(),
        params: BinanceWebsocketSignedParams {
            unsigned: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
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

fn to_weight(request: &BinanceWebsocketRequest) -> u32 {
    request.params.unsigned.weight()
}

fn to_order_count(request: &BinanceWebsocketRequest) -> u32 {
    request.params.unsigned.order_count()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::clock::Clock;
    use exchange_types::binance::{
        exchange_info::{
            BinanceExchangeInfoParams, BinanceExchangeInfoPermission,
            BinanceExchangeInfoSymbolStatus,
        },
        logon::BinanceLogonParams,
        time::BinanceTimeResult,
        websocket::BinanceWebsocketUnsignedRequest,
    };
    use std::{sync::Arc, time::Duration};

    #[test]
    fn request_weights_match_binance_docs() {
        let logon = BinanceWebsocketUnsignedRequest {
            id: "1".into(),
            params: BinanceWebsocketUnsignedParams::Logon(BinanceLogonParams { timestamp: 0 }),
        };
        assert_eq!(logon.params.weight(), 2);
        let exchange_info = BinanceWebsocketUnsignedRequest {
            id: "2".into(),
            params: BinanceWebsocketUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec![BinanceExchangeInfoPermission::SPOT],
                symbolStatus: BinanceExchangeInfoSymbolStatus::TRADING,
            }),
        };
        assert_eq!(exchange_info.params.weight(), 20);
        let time = BinanceWebsocketUnsignedRequest {
            id: "3".into(),
            params: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
        };
        assert_eq!(time.params.weight(), 1);
    }

    #[test]
    fn sync_lock_syncs_the_server_clock() {
        let clock = Arc::new(Clock::default());
        let synchronization = synchronization(Duration::from_secs(20));
        let request = (synchronization.create_time_request)();
        assert!(matches!(
            request.params,
            BinanceWebsocketSignedParams {
                unsigned: BinanceWebsocketUnsignedParams::Time(BinanceTimeParams {}),
                signature: None,
            }
        ));
        // The sync clock is unsigned: the request carries no payload to sign
        // (the transport sends it without an API key).
        let id = request.id;
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
}
