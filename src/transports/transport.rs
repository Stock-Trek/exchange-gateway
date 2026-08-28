use crate::{
    error::EGResult,
    functions::ArcPredicate,
    transports::{http::HttpTransport, websocket::WebsocketTransport},
};
use async_trait::async_trait;
use std::time::Duration;

pub(crate) enum Transport<EGReq, TransportReq, TransportRes, EGRes> {
    Http(HttpTransport<EGReq, TransportReq, TransportRes, EGRes>),
    Websocket(WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>),
}

#[async_trait]
pub(crate) trait TransportTrait<EGReq, TransportReq, TransportRes, EGRes> {
    fn try_convert_request(&self, request: EGReq) -> EGResult<TransportReq>;
    fn try_convert_response(&self, response_dto: TransportRes) -> EGResult<EGRes>;
    async fn connect(&self) -> EGResult<()>;
    fn is_connected(&self) -> bool;
    async fn fire_and_forget(&self, request: EGReq, timeout: Duration) -> EGResult<()>;
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes>;
    async fn disconnect(&self) -> EGResult<()>;
}

#[async_trait]
impl<EGReq, TransportReq, TransportRes, EGRes>
    TransportTrait<EGReq, TransportReq, TransportRes, EGRes>
    for Transport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    EGRes: Send + Sync + 'static,
{
    fn try_convert_request(&self, request: EGReq) -> EGResult<TransportReq> {
        match self {
            Self::Http(transport) => transport.try_convert_request(request),
            Self::Websocket(transport) => transport.try_convert_request(request),
        }
    }
    fn try_convert_response(&self, response: TransportRes) -> EGResult<EGRes> {
        match self {
            Self::Http(transport) => transport.try_convert_response(response),
            Self::Websocket(transport) => transport.try_convert_response(response),
        }
    }
    async fn connect(&self) -> EGResult<()> {
        match self {
            Self::Http(transport) => transport.connect().await,
            Self::Websocket(transport) => transport.connect().await,
        }
    }
    fn is_connected(&self) -> bool {
        match self {
            Self::Http(transport) => transport.is_connected(),
            Self::Websocket(transport) => transport.is_connected(),
        }
    }
    async fn fire_and_forget(&self, request: EGReq, timeout: Duration) -> EGResult<()> {
        match self {
            Self::Http(transport) => transport.fire_and_forget(request, timeout).await,
            Self::Websocket(transport) => transport.fire_and_forget(request, timeout).await,
        }
    }
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes> {
        match self {
            Self::Http(transport) => transport.send_and_wait_for(request, timeout, filter).await,
            Self::Websocket(transport) => {
                transport.send_and_wait_for(request, timeout, filter).await
            }
        }
    }
    async fn disconnect(&self) -> EGResult<()> {
        match self {
            Self::Http(transport) => transport.disconnect().await,
            Self::Websocket(transport) => transport.disconnect().await,
        }
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for Transport<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(transport) => f.debug_tuple("Transport::Http").field(transport).finish(),
            Self::Websocket(transport) => f
                .debug_tuple("Transport::Websocket")
                .field(transport)
                .finish(),
        }
    }
}
