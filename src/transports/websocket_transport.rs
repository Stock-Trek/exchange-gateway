use crate::{
    error::EGResult, listeners::listener::Listener, transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;

pub struct WebsocketTransportCreator {
    pub create_client: Box<dyn Fn(Listener<WebsocketMessageDto>) -> WebsocketClient>,
}

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

pub(crate) struct WebsocketTransport {
    client: WebsocketClient,
}

#[async_trait]
impl TransportTrait for WebsocketTransport {
    type MessageDto = WebsocketMessageDto;
    async fn send(&self, message: Self::MessageDto, timeout: Duration) -> EGResult<()> {
        self.client.send_message(message, timeout).await
    }
}

impl WebsocketTransport {
    pub fn new(client: crate::transports::websocket_transport::WebsocketClient) -> Self {
        Self { client }
    }
    pub async fn send_and_wait<TResponse, F>(
        &self,
        dto: WebsocketMessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&WebsocketMessageDto) -> Option<TResponse> + Send + Sync,
    {
        self.client.send_message(dto, timeout).await?;
        Err(crate::error::EGError::Custom(
            "send_and_wait for Websocket requires a response queue; \
             pass a QueueListener<WebsocketMessageDto> during construction \
             and poll it here"
                .into(),
        ))
    }
}
