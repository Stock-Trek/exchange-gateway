use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertRef, ArcTryConvertValue},
    listeners::listener::ListenerTrait,
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
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
    /// Local rate limiter realigned with server feedback on every response.
    rate_limits: RateLimits,
    /// Extracts exchange-level rate-limit feedback (e.g. `exchangeInfo`'s
    /// `rateLimits`) from a converted response.
    feedback: ArcTryConvertRef<EGRes, RateLimitFeedback>,
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
        self.listener.on_message(response).await?;
        Ok(())
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
        convert_request: ArcTryConvertValue<EGReq, TransportReq>,
        convert_response: ArcTryConvertValue<TransportRes, EGRes>,
        listener: Arc<dyn ListenerTrait<TMessage = EGRes>>,
        to_http_endpoint: fn(&EGReq) -> HttpEndpoint,
        endpoints: HashMap<HttpEndpoint, String>,
        rate_limits: RateLimits,
        feedback: impl Fn(&EGRes) -> EGResult<RateLimitFeedback> + Send + Sync + 'static,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
            listener,
            to_http_endpoint,
            endpoints,
            rate_limits,
            feedback: Arc::new(feedback),
            is_connected: AtomicBool::new(false),
        }
    }
    /// Sends the request, applies any server-side rate-limit feedback to the
    /// local limiter, and converts the response.
    ///
    /// A response (or rejection) carrying retry feedback — 429/418 or a
    /// `Retry-After` header — is surfaced as [`EGError::RateLimited`] rather
    /// than returned as a success, so callers never have to inspect feedback
    /// alongside a response.
    async fn to_converted_response(&self, request: EGReq, timeout: Duration) -> EGResult<EGRes> {
        let http_endpoint = (self.to_http_endpoint)(&request);
        let endpoint = self
            .endpoints
            .get(&http_endpoint)
            .ok_or(EGError::UnknownEndpoint)?;
        let request_dto = self.try_convert_request(request)?;
        let response_dto = match self
            .client
            .send_message(endpoint, request_dto, timeout)
            .await
        {
            Ok(response_dto) => response_dto,
            Err(error) => {
                // A 429/418 rejection carries throttling + usage feedback that
                // must still be applied: the request consumed server-side
                // weight and the response reports the true usage.
                let _ = self.rate_limits.apply_feedback_from_error(&error);
                return Err(error);
            }
        };
        let header_feedback = self.client.rate_limit_feedback(&response_dto);
        let response = self.try_convert_response(response_dto)?;
        let mut feedback = header_feedback;
        let exchange_feedback = (self.feedback)(&response)?;
        feedback.usage.extend(exchange_feedback.usage);
        feedback.retry_after = feedback.retry_after.or(exchange_feedback.retry_after);
        feedback.throttled |= exchange_feedback.throttled;
        self.rate_limits.apply_feedback(&feedback)?;
        if feedback.has_retry_feedback() {
            return Err(EGError::RateLimited { feedback });
        }
        Ok(response)
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
            .field("rate_limits", &self.rate_limits)
            .field("feedback", &"<function>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::EGError,
        listeners::listener::ListenerTrait,
        rate_limit::{
            feedback::RateLimitUsage, rate_limit_config::RateLimitConfig,
            rate_limit_type::RateLimitType, rate_limiter::RateLimiter,
        },
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
                    rate_limit_type: RateLimitType::RequestWeight,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                    used: response.used,
                    limit: None,
                }],
                ..Default::default()
            }
        }
    }

    /// A client whose responses carry retry feedback (429/418 or
    /// `Retry-After`), which must be interpreted as an error.
    struct ThrottledClient;

    #[async_trait]
    impl HttpClientTrait for ThrottledClient {
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
                used: 60,
            })
        }

        fn rate_limit_feedback(&self, _response: &TestRes) -> RateLimitFeedback {
            RateLimitFeedback {
                retry_after: Some(Duration::from_secs(30)),
                ..Default::default()
            }
        }
    }

    /// A client that rejects the request with 429, mirroring how
    /// [`ReqwestHttpClient`] surfaces Binance's `TOO_MANY_REQUESTS`.
    struct RejectingClient;

    #[async_trait]
    impl HttpClientTrait for RejectingClient {
        type TransportReq = TestReq;
        type TransportRes = TestRes;

        async fn send_message(
            &self,
            _endpoint: &str,
            _message: TestReq,
            _timeout: Duration,
        ) -> EGResult<TestRes> {
            Err(EGError::RateLimited {
                feedback: RateLimitFeedback {
                    throttled: true,
                    retry_after: Some(Duration::from_secs(30)),
                    usage: vec![RateLimitUsage {
                        rate_limit_type: RateLimitType::RequestWeight,
                        interval_nanos: Duration::from_secs(60).as_nanos(),
                        used: 6000,
                        limit: None,
                    }],
                },
            })
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

    fn rate_limits() -> RateLimits {
        RateLimits {
            weight: RateLimiter::new(vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 100,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }]),
            orders: RateLimiter::new(vec![]),
        }
    }

    fn transport() -> (
        HttpTransport<TestReq, TestReq, TestRes, TestRes>,
        RateLimits,
    ) {
        let mut endpoints = HashMap::new();
        endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(UsageClient),
            Arc::new(Ok),
            Arc::new(Ok),
            Arc::new(NoopListener),
            |_| HttpEndpoint::ExchangeInfo,
            endpoints,
            rate_limits.clone(),
            |_: &TestRes| Ok(RateLimitFeedback::default()),
        );
        (transport, rate_limits)
    }

    fn throttled_transport() -> (
        HttpTransport<TestReq, TestReq, TestRes, TestRes>,
        RateLimits,
    ) {
        let mut endpoints = HashMap::new();
        endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(ThrottledClient),
            Arc::new(Ok),
            Arc::new(Ok),
            Arc::new(NoopListener),
            |_| HttpEndpoint::ExchangeInfo,
            endpoints,
            rate_limits.clone(),
            |_: &TestRes| Ok(RateLimitFeedback::default()),
        );
        (transport, rate_limits)
    }

    #[tokio::test]
    async fn send_and_wait_applies_feedback_and_returns_response() {
        let (transport, rate_limits) = transport();
        assert!(rate_limits.weight.did_acquire(40).unwrap());
        let response = transport
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|response: &TestRes| response.id == 1),
            )
            .await
            .expect("matching response should be returned");
        assert_eq!(response, TestRes { id: 1, used: 42 });
        // The usage reported by the server is applied to the local limiter:
        // remaining capacity is trimmed to 100 - 42 = 58.
        assert!(rate_limits.weight.did_acquire(58).unwrap());
        assert!(!rate_limits.weight.did_acquire(1).unwrap());
    }

    #[tokio::test]
    async fn send_and_wait_interprets_retry_feedback_as_error() {
        let (transport, rate_limits) = throttled_transport();
        let error = transport
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|response: &TestRes| response.id == 1),
            )
            .await
            .expect_err("retry feedback should be an error");
        match error {
            EGError::RateLimited { feedback } => {
                assert_eq!(feedback.retry_after, Some(Duration::from_secs(30)));
            }
            other => panic!("expected RateLimited, got: {other:?}"),
        }
        // The retry feedback drained the bucket until Retry-After elapses.
        assert!(!rate_limits.weight.did_acquire(1).unwrap());
    }

    #[tokio::test]
    async fn send_and_wait_rejects_non_matching_response() {
        let (transport, _) = transport();
        let error = transport
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|response: &TestRes| response.id == 99),
            )
            .await
            .expect_err("non-matching response should be an error");
        assert!(matches!(error, EGError::BadResponse));
    }

    #[tokio::test]
    async fn fire_and_forget_forwards_response_to_listener() {
        let (transport, _) = transport();
        transport
            .fire_and_forget(TestReq { id: 1 }, Duration::from_secs(5))
            .await
            .expect("fire and forget should succeed");
    }

    #[tokio::test]
    async fn fire_and_forget_interprets_retry_feedback_as_error() {
        let (transport, _) = throttled_transport();
        let error = transport
            .fire_and_forget(TestReq { id: 1 }, Duration::from_secs(5))
            .await
            .expect_err("retry feedback should be an error");
        assert!(matches!(error, EGError::RateLimited { .. }));
    }

    #[tokio::test]
    async fn rejected_request_applies_feedback_from_error() {
        let mut endpoints = HashMap::new();
        endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(RejectingClient),
            Arc::new(Ok),
            Arc::new(Ok),
            Arc::new(NoopListener),
            |_| HttpEndpoint::ExchangeInfo,
            endpoints,
            rate_limits.clone(),
            |_: &TestRes| Ok(RateLimitFeedback::default()),
        );
        assert!(rate_limits.weight.did_acquire(10).unwrap());
        let error = transport
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|response: &TestRes| response.id == 1),
            )
            .await
            .expect_err("429 should be an error");
        assert!(matches!(error, EGError::RateLimited { .. }));
        // The rejection's feedback was applied even though the request failed:
        // the bucket is drained until Retry-After elapses.
        assert!(!rate_limits.weight.did_acquire(1).unwrap());
    }
}
