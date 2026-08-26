use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
    rate_limit::feedback::RateLimitFeedback,
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

    /// Extracts server-side rate-limit feedback from a response, if any.
    ///
    /// Clients that surface exchange rate-limit headers (e.g. Binance's
    /// `Retry-After` and `X-MBX-*` headers) override this so the gateway can
    /// feed the server's view back into the local rate limiter. The default
    /// returns no feedback.
    fn rate_limit_feedback(&self, _response: &Self::TransportRes) -> RateLimitFeedback {
        RateLimitFeedback::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpEndpoint {
    AssetLimits,
    ExchangeInfo,
    PlaceOrder,
}

pub(crate) struct HttpTransport<EGReq, TransportReq, TransportRes, EGRes> {
    client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
    convert_request: ArcTryConvertValue<EGReq, TransportReq>,
    convert_response: ArcTryConvertValue<TransportRes, EGRes>,
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
    async fn fire_and_forget(
        &self,
        request: EGReq,
        timeout: Duration,
    ) -> EGResult<RateLimitFeedback> {
        let (response, feedback) = self.to_converted_response(request, timeout).await?;
        self.listener.on_message(response).await?;
        Ok(feedback)
    }
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes> {
        let (response, _) = self.to_converted_response(request, timeout).await?;
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
        convert_request: ArcTryConvertValue<EGReq, TransportReq>,
        convert_response: ArcTryConvertValue<TransportRes, EGRes>,
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
    async fn to_converted_response(
        &self,
        request: EGReq,
        timeout: Duration,
    ) -> EGResult<(EGRes, RateLimitFeedback)> {
        let http_endpoint = (self.to_http_endpoint)(&request);
        let endpoint = self
            .endpoints
            .get(&http_endpoint)
            .ok_or(EGError::UnknownEndpoint)?;
        let request_dto = self.try_convert_request(request)?;
        let response_dto = self
            .client
            .send_message(endpoint, request_dto, timeout)
            .await?;
        let feedback = self.client.rate_limit_feedback(&response_dto);
        let response = self.try_convert_response(response_dto)?;
        Ok((response, feedback))
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for HttpTransport<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransport")
            .field("client", &"<HttpClientTrait>")
            .field("convert_request", &"<function>")
            .field("convert_response", &"<function>")
            .field("listener", &"<Listener>")
            .field("to_http_endpoint", &self.to_http_endpoint)
            .field("action_endpoints", &self.endpoints)
            .finish()
    }
}
