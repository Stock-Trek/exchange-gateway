use crate::error::{EGError, EGResult};
use async_trait::async_trait;
use chrono::Duration;
use std::pin::Pin;

#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;
    async fn send(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
    ) -> EGResult<Self::MessageDto>;
}

pub struct TransportImpl<TransportMessage, MessageDto> {
    transporter:
        fn(TransportMessage, Duration) -> Box<dyn Future<Output = TransportMessage> + Send>,
    serializer: fn(MessageDto) -> TransportMessage,
    deserializer: fn(TransportMessage) -> EGResult<MessageDto>,
}

impl<TransportMessage, MessageDto> TransportImpl<TransportMessage, MessageDto> {
    pub fn new(
        transporter: fn(
            TransportMessage,
            Duration,
        ) -> Box<dyn Future<Output = TransportMessage> + Send>,
        serializer: fn(MessageDto) -> TransportMessage,
        deserializer: fn(TransportMessage) -> EGResult<MessageDto>,
    ) -> Self {
        Self {
            transporter,
            serializer,
            deserializer,
        }
    }
}

#[async_trait]
impl<TransportMessage, MessageDto> TransportTrait for TransportImpl<TransportMessage, MessageDto>
where
    MessageDto: Send + Sync,
{
    type MessageDto = MessageDto;
    async fn send(&self, message_dto: MessageDto, timeout: Duration) -> EGResult<MessageDto> {
        let transport_message = (self.serializer)(message_dto);
        let future = (self.transporter)(transport_message, timeout);
        let pinned_future = Pin::from(future);
        let result = pinned_future.await;
        match (self.deserializer)(result) {
            Err(_e) => Err(EGError::Custom("".to_string())),
            Ok(message) => Ok(message),
        }
    }
}
