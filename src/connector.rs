use crate::error::EGResult;
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait Connector {
    type Request;
    type Response;

    async fn connect(&self) -> EGResult<()>;
    async fn sync_clock(&self) -> EGResult<()>;
    fn is_connected(&self) -> EGResult<bool>;
    async fn send(&self, request: Self::Request, timeout: Duration) -> EGResult<Self::Response>;
    async fn disconnect(&self) -> EGResult<()>;
}
