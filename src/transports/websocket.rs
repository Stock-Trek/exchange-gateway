use crate::{error::EGResult, listeners::listener::Listener};
use async_trait::async_trait;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc, time::Duration};

pub type CreateWebsocketClient<T> =
    Arc<dyn Fn(&str, Listener<WebsocketMessageDto<T>>) -> WebsocketClientMarker<T> + Send + Sync>;

pub type WebsocketClientMarker<T> = Arc<dyn WebsocketClientTrait<T>>;

#[async_trait]
pub trait WebsocketClientTrait<T>: Send + Sync {
    async fn connect(&self) -> EGResult<()>;
    async fn send_message(
        &self,
        message: WebsocketMessageDto<T>,
        timeout: Duration,
    ) -> EGResult<()>;
    fn is_connected(&self) -> bool;
    async fn disconnect(&self) -> EGResult<()>;
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WebsocketMessageDto<T> {
    pub body: T,
}

impl<T> std::fmt::Display for WebsocketMessageDto<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebsocketMessageDto( body: {} )", self.body)
    }
}
