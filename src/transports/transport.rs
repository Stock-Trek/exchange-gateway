use crate::error::EGResult;
use async_trait::async_trait;
use chrono::Duration;

#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;
    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()>;
}
