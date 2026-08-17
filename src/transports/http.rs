use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, TryConvertValue},
    listeners::listener::ListenerTrait,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

#[async_trait]
pub trait HttpClientTrait: Send + Sync {
    type TransportReq;
    type TransportRes;

    async fn send_message(
        &self,
        message: Self::TransportReq,
        timeout: Duration,
    ) -> EGResult<Self::TransportRes>;
}

pub(crate) struct HttpTransport<EGReq, TransportReq, TransportRes, EGRes> {
    client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
    convert_request: TryConvertValue<EGReq, TransportReq>,
    convert_response: TryConvertValue<TransportRes, EGRes>,
    listener: Arc<dyn ListenerTrait<TMessage = EGRes>>,
}

#[async_trait]
impl<EGReq, TransportReq, TransportRes, EGRes>
    TransportTrait<EGReq, TransportReq, TransportRes, EGRes>
    for HttpTransport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    EGRes: 'static,
{
    fn try_convert_request(&self, request: EGReq) -> EGResult<TransportReq> {
        (self.convert_request)(request)
    }
    fn try_convert_response(&self, response: TransportRes) -> EGResult<EGRes> {
        (self.convert_response)(response)
    }
    async fn connect(&self) -> EGResult<()> {
        Ok(())
    }
    fn is_connected(&self) -> bool {
        true
    }
    async fn fire_and_forget(&self, request: EGReq, timeout: Duration) -> EGResult<()> {
        let request_dto = self.try_convert_request(request)?;
        let response_dto = self.client.send_message(request_dto, timeout).await?;
        let response = self.try_convert_response(response_dto)?;
        self.listener.on_message(response).await
    }
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes> {
        let request_dto = self.try_convert_request(request)?;
        let response_dto = self.client.send_message(request_dto, timeout).await?;
        let response = self.try_convert_response(response_dto)?;
        if (filter)(&response) {
            Ok(response)
        } else {
            Err(EGError::BadResponse)
        }
    }
    async fn disconnect(&self) -> EGResult<()> {
        Ok(())
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes>
    HttpTransport<EGReq, TransportReq, TransportRes, EGRes>
{
    pub fn new(
        client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
        convert_request: TryConvertValue<EGReq, TransportReq>,
        convert_response: TryConvertValue<TransportRes, EGRes>,
        listener: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
            listener,
        }
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for HttpTransport<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransport")
            .field("client", &"<HttpClientTrait>")
            .field("convert_request", &self.convert_request)
            .field("convert_response", &self.convert_response)
            .field("listener", &"<Listener>")
            .finish()
    }
}
