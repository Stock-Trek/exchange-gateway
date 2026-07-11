use crate::{error::EGResult, listeners::listener::Listener};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

pub type CreateWebsocketClient = Arc<dyn Fn(Listener<WebsocketMessageDto>) -> WebsocketClient>;

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
