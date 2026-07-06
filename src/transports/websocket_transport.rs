use crate::{
    error::EGResult,
    listeners::listener::Listener,
    transports::{transport::TransportTrait, transport_creator::TransportCreatorTrait},
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

impl TransportCreatorTrait<WebsocketTransport, WebsocketMessageDto> for WebsocketTransportCreator {
    fn create_transport(
        &self,
        listener: Listener<WebsocketMessageDto>,
    ) -> EGResult<WebsocketTransport> {
        let client = (self.create_client)(listener);
        client.start_listening()?;
        Ok(WebsocketTransport { client })
    }
}

#[derive(Clone)]
pub struct WebsocketMessageDto {
    pub body_json: String,
}

pub(crate) struct WebsocketTransport {
    client: WebsocketClient,
}

impl TransportTrait for WebsocketTransport {
    type MessageDto = WebsocketMessageDto;
}

impl WebsocketTransport {
    pub fn new(client: crate::transports::websocket_transport::WebsocketClient) -> Self {
        Self { client }
    }
    pub async fn send(&self, message: WebsocketMessageDto, timeout: Duration) -> EGResult<()> {
        self.client.send_message(message, timeout).await
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
