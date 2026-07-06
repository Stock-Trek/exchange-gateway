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

pub struct WebsocketTransport {
    client: WebsocketClient,
}

#[async_trait]
impl TransportTrait for WebsocketTransport {
    type MessageDto = WebsocketMessageDto;

    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()> {
        self.send_inner(message_dto, timeout).await
    }

    async fn send_and_wait<TResponse, F>(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&Self::MessageDto) -> Option<TResponse> + Send + Sync,
        TResponse: Send + Sync,
    {
        self.send_and_wait_inner(message_dto, timeout, filter).await
    }
}

impl WebsocketTransport {
    pub(crate) async fn send_inner(
        &self,
        message: WebsocketMessageDto,
        timeout: Duration,
    ) -> EGResult<()> {
        self.client.send_message(message, timeout).await
    }
    pub(crate) async fn send_and_wait_inner<TResponse, F>(
        &self,
        dto: WebsocketMessageDto,
        timeout: Duration,
        #[allow(unused)] filter: F,
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
