use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, TryConvertValue},
    listeners::listener::ListenerTrait,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[async_trait]
pub trait HttpClientTrait: Send + Sync {
    type TransportReq;
    type TransportRes;

    async fn send_message(
        &self,
        endpoint: &str,
        message: Self::TransportReq,
        timeout: Duration,
    ) -> EGResult<Self::TransportRes>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpEndpoint {
    AssetLimits,
    ExchangeInfo,
    PlaceOrder,
}

pub(crate) struct HttpTransport<EGReq, TransportReq, TransportRes, EGRes> {
    client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
    convert_request: TryConvertValue<EGReq, TransportReq>,
    convert_response: TryConvertValue<TransportRes, EGRes>,
    listener: Arc<dyn ListenerTrait<TMessage = EGRes>>,
    to_http_endpoint: fn(&EGReq) -> HttpEndpoint,
    endpoints: HashMap<HttpEndpoint, String>,
    is_connected: AtomicBool,
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
        self.is_connected.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::SeqCst)
    }
    async fn fire_and_forget(&self, request: EGReq, timeout: Duration) -> EGResult<()> {
        let response = self.to_converted_response(request, timeout).await?;
        self.listener.on_message(response).await
    }
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes> {
        let response = self.to_converted_response(request, timeout).await?;
        if (filter)(&response) {
            Ok(response)
        } else {
            Err(EGError::BadResponse)
        }
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.is_connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes>
    HttpTransport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    EGRes: 'static,
{
    pub fn new(
        client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
        convert_request: TryConvertValue<EGReq, TransportReq>,
        convert_response: TryConvertValue<TransportRes, EGRes>,
        listener: Arc<dyn ListenerTrait<TMessage = EGRes>>,
        to_http_endpoint: fn(&EGReq) -> HttpEndpoint,
        endpoints: HashMap<HttpEndpoint, String>,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
            listener,
            to_http_endpoint,
            endpoints,
            is_connected: AtomicBool::new(false),
        }
    }
    async fn to_converted_response(&self, request: EGReq, timeout: Duration) -> EGResult<EGRes> {
        let http_endpoint = (self.to_http_endpoint)(&request);
        let endpoint = self
            .endpoints
            .get(&http_endpoint)
            .map_or("", String::as_str);
        let request_dto = self.try_convert_request(request)?;
        let response_dto = self
            .client
            .send_message(endpoint, request_dto, timeout)
            .await?;
        self.try_convert_response(response_dto)
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
            .field("to_http_endpoint", &self.to_http_endpoint)
            .field("action_endpoints", &self.endpoints)
            .finish()
    }
}
