use crate::{error::EGResult, functions::ArcPredicate};
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait Connector<ExternalReq, ExternalRes> {
    async fn connect(&self) -> EGResult<()>;
    fn is_connected(&self) -> EGResult<bool>;
    fn is_authenticated(&self) -> EGResult<bool>;
    /// Sends a request and waits for, then returns, the matching response.
    ///
    /// `filter` is used to correlate the response to this request for
    /// transports where responses arrive out-of-band (e.g. websockets). It
    /// should return `true` for exactly the response that belongs to
    /// `request`. For transports where the response is returned synchronously
    /// (e.g. HTTP) the filter is ignored.
    async fn send(
        &self,
        request: ExternalReq,
        signed: bool,
        timeout: Duration,
        filter: ArcPredicate<ExternalRes>,
    ) -> EGResult<ExternalRes>;
    async fn disconnect(&self) -> EGResult<()>;
}
