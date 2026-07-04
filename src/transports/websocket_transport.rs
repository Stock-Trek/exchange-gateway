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

#[async_trait]
impl TransportTrait for WebsocketTransport {
    type MessageDto = WebsocketMessageDto;
    async fn send(&self, message: Self::MessageDto, timeout: Duration) -> EGResult<()> {
        self.client.send_message(message, timeout).await
    }
}
