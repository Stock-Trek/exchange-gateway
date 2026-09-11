use crate::error::{EGResult, SendResult};
use async_trait::async_trait;
use exchange_types::http::{HttpRequest, HttpResponse};
use std::time::Duration;

#[async_trait]
pub trait HttpClient {
    async fn send(&self, request: HttpRequest, timeout: Duration) -> SendResult<HttpResponse>;
}

#[async_trait]
pub trait WebsocketClient: Sized + Send + Sync {
    async fn connect(&self) -> EGResult<()>;
    fn is_connected(&self) -> bool;
    async fn send(&self, message: String, timeout: Duration) -> SendResult<()>;
    async fn disconnect(&self) -> EGResult<()>;
}
