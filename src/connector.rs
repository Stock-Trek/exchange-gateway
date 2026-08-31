use crate::error::EGResult;
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait Connector<ExternalReq, ExternalRes> {
    async fn connect(&self) -> EGResult<()>;
    async fn sync_clock(&self) -> EGResult<()>;
    async fn authenticate(&self) -> EGResult<()>;
    fn is_connected(&self) -> EGResult<bool>;
    fn is_authenticated(&self) -> EGResult<bool>;
    async fn send(
        &self,
        request: ExternalReq,
        signed: bool,
        timeout: Duration,
    ) -> EGResult<ExternalRes>;
    async fn disconnect(&self) -> EGResult<()>;
}
