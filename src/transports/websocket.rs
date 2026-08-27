use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue},
    listeners::websocket_listener::WebsocketListener,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use futures_timer::Delay;
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

#[async_trait]
pub(crate) trait WebsocketClientTrait: Send + Sync {
    type TransportReq;
    type TransportRes;

    async fn connect(&self) -> EGResult<()>;
    fn is_connected(&self) -> bool;
    async fn send_message(&self, message: Self::TransportReq, timeout: Duration) -> EGResult<()>;
    async fn disconnect(&self) -> EGResult<()>;
}

pub(crate) struct WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes> {
    client: Arc<dyn WebsocketClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
    convert_request: ArcTryConvertValue<EGReq, TransportReq>,
    convert_response: ArcTryConvertValue<TransportRes, EGRes>,
    websocket_listener: Arc<WebsocketListener<TransportRes, EGRes>>,
}

#[async_trait]
impl<EGReq, TransportReq, TransportRes, EGRes>
    TransportTrait<EGReq, TransportReq, TransportRes, EGRes>
    for WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    EGRes: Send + Sync + 'static,
{
    fn try_convert_request(&self, request: EGReq) -> EGResult<TransportReq> {
        (self.convert_request)(request)
    }
    fn try_convert_response(&self, response: TransportRes) -> EGResult<EGRes> {
        (self.convert_response)(response)
    }
    async fn connect(&self) -> EGResult<()> {
        self.client.connect().await
    }
    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }
    async fn fire_and_forget(&self, request: EGReq, timeout: Duration) -> EGResult<()> {
        let transport_req = self.try_convert_request(request)?;
        self.client.send_message(transport_req, timeout).await?;
        Ok(())
    }
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes> {
        let transport_req = self.try_convert_request(request)?;
        let waiter = self
            .websocket_listener
            .waiter_for_filtered_response(filter)?;
        self.client.send_message(transport_req, timeout).await?;
        self.wait_for_response(waiter, timeout).await
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.client.disconnect().await
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes>
    WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub fn new(
        client: Arc<
            dyn WebsocketClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>,
        >,
        convert_request: ArcTryConvertValue<EGReq, TransportReq>,
        convert_response: ArcTryConvertValue<TransportRes, EGRes>,
        websocket_listener: Arc<WebsocketListener<TransportRes, EGRes>>,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
            websocket_listener,
        }
    }
    fn wait_for_response(
        &self,
        waiter: impl Future<Output = EGResult<EGRes>> + Send + 'static,
        timeout: Duration,
    ) -> impl Future<Output = EGResult<EGRes>> + Send + 'static {
        let mut waiter = Box::pin(waiter);
        let mut delay = Box::pin(Delay::new(timeout));
        poll_fn(move |cx| match waiter.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketTransport")
            .field("client", &"<HttpClientTrait>")
            .field("convert_request", &"<function>")
            .field("convert_response", &"<function>")
            .field("websocket_listener", &self.websocket_listener)
            .finish()
    }
}
