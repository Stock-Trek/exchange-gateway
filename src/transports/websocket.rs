use crate::{error::EGResult, listeners::listener::Listener};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

pub type CreateWebsocketClient =
    Arc<dyn Fn(&str, Listener<WebsocketMessageDto>) -> WebsocketClientMarker + Send + Sync>;

pub type WebsocketClientMarker = Arc<dyn WebsocketClientTrait>;

#[async_trait]
pub trait WebsocketClientTrait: Send + Sync {
    async fn connect(&self) -> EGResult<()>;
    async fn send_message(&self, message: WebsocketMessageDto, timeout: Duration) -> EGResult<()>;
    fn is_connected(&self) -> bool;
    async fn disconnect(&self) -> EGResult<()>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WebsocketMessageDto {
    pub body_json: String,
}
