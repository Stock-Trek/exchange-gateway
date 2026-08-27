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
pub(crate) trait HttpClientTrait: Send + Sync {
    type TransportReq;
    type TransportRes;

    /// Sends a transport-level request and returns the response.
    ///
    /// Implementations must surface non-success HTTP statuses as [`EGError`]
    /// rather than returning them as successful responses: 429 should map to
    /// [`EGError::RateLimited`] and other non-2xx statuses to
    /// [`EGError::HttpError`] so that callers do not have to inspect status
    /// codes themselves.
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
pub(crate) enum HttpEndpoint {
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
    ) -> EGResult<(EGRes, RateLimitFeedback)> {
        let (response, feedback) = self.to_converted_response(request, timeout).await?;
        if (filter)(&response) {
            Ok((response, feedback))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::EGError, listeners::listener::ListenerTrait, rate_limit::feedback::RateLimitUsage,
    };
    use std::time::Duration;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestReq {
        id: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestRes {
        id: u64,
        used: u32,
    }

    /// A client that answers with a usage report, mirroring Binance's
    /// `X-MBX-*` headers.
    struct UsageClient;

    #[async_trait]
    impl HttpClientTrait for UsageClient {
        type TransportReq = TestReq;
        type TransportRes = TestRes;

        async fn send_message(
            &self,
            _endpoint: &str,
            message: TestReq,
            _timeout: Duration,
        ) -> EGResult<TestRes> {
            Ok(TestRes {
                id: message.id,
                used: 42,
            })
        }

        fn rate_limit_feedback(&self, response: &TestRes) -> RateLimitFeedback {
            RateLimitFeedback {
                usage: vec![RateLimitUsage {
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                    used: response.used,
                    limit: None,
                }],
                ..Default::default()
            }
        }
    }

    struct NoopListener;

    #[async_trait]
    impl ListenerTrait for NoopListener {
        type TMessage = TestRes;

        async fn on_message(&self, _message: TestRes) -> EGResult<()> {
            Ok(())
        }
    }

    fn transport() -> HttpTransport<TestReq, TestReq, TestRes, TestRes> {
        let mut endpoints = HashMap::new();
        endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
        HttpTransport::new(
            Arc::new(UsageClient),
            Arc::new(Ok),
            Arc::new(Ok),
            Arc::new(NoopListener),
            |_| HttpEndpoint::ExchangeInfo,
            endpoints,
        )
    }

    #[tokio::test]
    async fn send_and_wait_reports_feedback_with_the_response() {
        let (response, feedback) = transport()
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|response: &TestRes| response.id == 1),
            )
            .await
            .expect("matching response should be returned");
        assert_eq!(response, TestRes { id: 1, used: 42 });
        // The usage reported by the server travels back with the response so
        // the connector can realign the local limiter (previously discarded).
        assert_eq!(feedback.usage.len(), 1);
        assert_eq!(
            feedback.usage[0].interval_nanos,
            Duration::from_secs(60).as_nanos()
        );
        assert_eq!(feedback.usage[0].used, 42);
    }

    #[tokio::test]
    async fn send_and_wait_rejects_non_matching_response() {
        let error = transport()
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|response: &TestRes| response.id == 99),
            )
            .await
            .expect_err("non-matching response should be an error");
        assert!(matches!(error, EGError::BadResponse));
    }
}
