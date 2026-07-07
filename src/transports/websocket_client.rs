use crate::error::EGResult;
use async_trait::async_trait;
use std::time::Duration;

pub type WebsocketClient = Box<dyn WebsocketClientTrait>;

#[async_trait]
pub trait WebsocketClientTrait: Send + Sync {
    fn start_listening(&self) -> EGResult<()>;
    async fn send_message(&self, message: WebsocketMessageDto, timeout: Duration) -> EGResult<()>;
}

#[derive(Clone)]
pub struct WebsocketMessageDto {
    pub body_json: String,
}
