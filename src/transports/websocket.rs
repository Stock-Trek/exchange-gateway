use crate::{error::EGResult, listeners::listener::Listener};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

pub type CreateWebsocketClient =
    Arc<dyn Fn(Listener<WebsocketMessageDto>) -> WebsocketClientMarker>;

pub type WebsocketClientMarker = Box<dyn WebsocketClientTrait>;

#[async_trait]
pub trait WebsocketClientTrait: Send + Sync {
    fn start_listening(&self) -> EGResult<()>;
    async fn send_message(&self, message: WebsocketMessageDto, timeout: Duration) -> EGResult<()>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WebsocketMessageDto {
    pub body_json: String,
}
