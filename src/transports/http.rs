use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertRef, ArcTryConvertValue, TryConvertValue},
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use std::{
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

pub(crate) struct HttpTransport<EGReq, TransportReq, TransportRes, EGRes> {
    client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
    convert_request: ArcTryConvertValue<EGReq, TransportReq>,
    convert_response: TryConvertValue<TransportRes, EGRes>,
    to_endpoint: fn(&EGReq) -> String,
    rate_limits: RateLimits,
    header_feedback: Arc<dyn Fn(&TransportRes) -> RateLimitFeedback + Send + Sync>,
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
    pub(crate) fn new(
        client: Arc<dyn HttpClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
        convert_request: ArcTryConvertValue<EGReq, TransportReq>,
        convert_response: TryConvertValue<TransportRes, EGRes>,
        to_endpoint: fn(&EGReq) -> String,
        rate_limits: RateLimits,
        header_feedback: impl Fn(&TransportRes) -> RateLimitFeedback + Send + Sync + 'static,
        feedback: impl Fn(&EGRes) -> EGResult<RateLimitFeedback> + Send + Sync + 'static,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
            to_endpoint,
            rate_limits,
            header_feedback: Arc::new(header_feedback),
            feedback: Arc::new(feedback),
            is_connected: AtomicBool::new(false),
        }
    }
    async fn to_converted_response(&self, request: EGReq, timeout: Duration) -> EGResult<EGRes> {
        let endpoint = (self.to_endpoint)(&request);
        let request_dto = self.try_convert_request(request)?;
        let response_dto = match self
            .client
            .send_message(&endpoint, request_dto, timeout)
            .await
        {
            Ok(response_dto) => response_dto,
            Err(error) => {
                let _ = self.rate_limits.apply_feedback_from_error(&error);
                return Err(error);
            }
        };
        let mut feedback = (self.header_feedback)(&response_dto);
        let response = match self.try_convert_response(response_dto) {
            Ok(response) => {
                let exchange_feedback = (self.feedback)(&response)?;
                feedback.usage.extend(exchange_feedback.usage);
                feedback.retry_after = feedback.retry_after.or(exchange_feedback.retry_after);
                feedback.is_throttled |= exchange_feedback.is_throttled;
                response
            }
            Err(error) => {
                // The response body could not be converted (e.g. a 2xx
                // response carrying an error body, which Binance maps to an
                // API error). The transport-level header feedback (X-MBX-*
                // usage) is still valid, so apply it before surfacing the
                // conversion error.
                self.rate_limits.apply_feedback(&feedback)?;
                if feedback.has_retry_feedback() {
                    return Err(EGError::RateLimited(feedback));
                }
                return Err(error);
            }
        };
        self.rate_limits.apply_feedback(&feedback)?;
        if feedback.has_retry_feedback() {
            return Err(EGError::RateLimited(feedback));
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
            .field("to_endpoint", &"<function>")
            .field("rate_limits", &self.rate_limits)
            .field("header_feedback", &"<function>")
            .field("feedback", &"<function>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::{EGError, EGResult},
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
            Err(EGError::RateLimited(RateLimitFeedback {
                is_throttled: true,
                retry_after: Some(Duration::from_secs(30)),
                usage: vec![RateLimitUsage {
                    rate_limit_type: RateLimitType::RequestWeight,
                    interval_nanos: Duration::from_secs(60).as_nanos(),
                    used: Some(6000),
                    limit: None,
                }],
            }))
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
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(UsageClient),
            Arc::new(Ok),
            Ok,
            |_| "exchangeInfo".into(),
            rate_limits.clone(),
            header_feedback,
            |_: &TestRes| Ok(RateLimitFeedback::default()),
        );
        (transport, rate_limits)
    }

    fn header_feedback(response: &TestRes) -> RateLimitFeedback {
        RateLimitFeedback {
            usage: vec![RateLimitUsage {
                rate_limit_type: RateLimitType::RequestWeight,
                interval_nanos: Duration::from_secs(60).as_nanos(),
                used: Some(response.used),
                limit: None,
            }],
            ..Default::default()
        }
    }

    fn throttled_transport() -> (
        HttpTransport<TestReq, TestReq, TestRes, TestRes>,
        RateLimits,
    ) {
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(ThrottledClient),
            Arc::new(Ok),
            Ok,
            |_| "exchangeInfo".into(),
            rate_limits.clone(),
            |_: &TestRes| RateLimitFeedback {
                retry_after: Some(Duration::from_secs(30)),
                ..Default::default()
            },
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
            EGError::RateLimited(feedback) => {
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
    async fn send_and_wait_applies_header_feedback_when_body_converts_to_error() {
        // Regression test: Binance answers with a 2xx status and an error
        // body (e.g. `{"code":-2015,"msg":"Invalid API-key."}`), which
        // converts to an ApiError. The X-MBX-* header usage on that response
        // must still be fed back to the local limiter.
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(UsageClient),
            Arc::new(Ok),
            |_: TestRes| -> EGResult<TestRes> {
                Err(EGError::ApiError {
                    code: -2015,
                    message: "Invalid API-key.".into(),
                })
            },
            |_| "exchangeInfo".into(),
            rate_limits.clone(),
            header_feedback,
            |_: &TestRes| Ok(RateLimitFeedback::default()),
        );
        assert!(rate_limits.weight.did_acquire(40).unwrap());
        let error = transport
            .send_and_wait_for(
                TestReq { id: 1 },
                Duration::from_secs(5),
                Arc::new(|_: &TestRes| true),
            )
            .await
            .expect_err("2xx error body should surface as an ApiError");
        assert!(matches!(error, EGError::ApiError { .. }));
        // The usage reported by the server headers is applied even though
        // the response body mapped to an error: remaining capacity is
        // trimmed to 100 - 42 = 58.
        assert!(rate_limits.weight.did_acquire(58).unwrap());
        assert!(!rate_limits.weight.did_acquire(1).unwrap());
    }

    #[tokio::test]
    async fn rejected_request_applies_feedback_from_error() {
        let rate_limits = rate_limits();
        let transport = HttpTransport::new(
            Arc::new(RejectingClient),
            Arc::new(Ok),
            Ok,
            |_| "exchangeInfo".into(),
            rate_limits.clone(),
            |_: &TestRes| RateLimitFeedback::default(),
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
        assert!(matches!(error, EGError::RateLimited(..)));
        // The rejection's feedback was applied even though the request failed:
        // the bucket is drained until Retry-After elapses.
        assert!(!rate_limits.weight.did_acquire(1).unwrap());
    }
}
