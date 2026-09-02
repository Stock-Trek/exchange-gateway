use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue, TryConvertValue},
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
    convert_request: ArcTryConvertValue<EGReq, TransportReq>,
    convert_response: TryConvertValue<TransportRes, EGRes>,
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
        convert_response: TryConvertValue<TransportRes, EGRes>,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
        }
    }
    async fn to_converted_response(&self, request: EGReq, timeout: Duration) -> EGResult<EGRes> {
        let request_dto = (self.convert_request)(request)?;
        let response_dto = self.client.send_message(request_dto, timeout).await?;
        self.try_convert_response(response_dto)
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for HttpTransport<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTransport")
            .field("client", &"<HttpClientTrait>")
            .field("convert_request", &"<function>")
            .field("convert_response", &self.convert_response)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{EGError, EGResult};
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

        async fn send_message(&self, message: TestReq, _timeout: Duration) -> EGResult<TestRes> {
            Ok(TestRes {
                id: message.id,
                used: 42,
            })
        }
    }

    fn transport() -> HttpTransport<TestReq, TestReq, TestRes, TestRes> {
        HttpTransport::new(Arc::new(UsageClient), Arc::new(Ok), Ok)
    }

    #[tokio::test]
    async fn send_and_wait_rejects_non_matching_response() {
        let transport = transport();
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
}
