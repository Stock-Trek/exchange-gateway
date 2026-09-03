use crate::error::EGResult;
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait Connector<Request, Response> {
    async fn connect(&self) -> EGResult<()>;
    async fn sync_clock(&self) -> EGResult<()>;
    fn server_time_millis(&self) -> EGResult<i64>;
    fn is_connected(&self) -> EGResult<bool>;
    async fn send(&self, request: Request, timeout: Duration) -> EGResult<Response>;
    async fn disconnect(&self) -> EGResult<()>;
}
