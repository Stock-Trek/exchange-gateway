use async_trait::async_trait;
use chrono::Duration;
use std::pin::Pin;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;
    async fn send(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
    ) -> StockTrekResult<Self::MessageDto>;
}

pub struct TransportImpl<TransportMessage, MessageDto> {
    transporter:
        fn(TransportMessage, Duration) -> Box<dyn Future<Output = TransportMessage> + Send>,
    serializer: fn(MessageDto) -> TransportMessage,
    deserializer: fn(TransportMessage) -> StockTrekResult<MessageDto>,
}

impl<TransportMessage, MessageDto> TransportImpl<TransportMessage, MessageDto> {
    pub fn new(
        transporter: fn(
            TransportMessage,
            Duration,
        ) -> Box<dyn Future<Output = TransportMessage> + Send>,
        serializer: fn(MessageDto) -> TransportMessage,
        deserializer: fn(TransportMessage) -> StockTrekResult<MessageDto>,
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
    async fn send(
        &self,
        message_dto: MessageDto,
        timeout: Duration,
    ) -> StockTrekResult<MessageDto> {
        let transport_message = (self.serializer)(message_dto);
        let future = (self.transporter)(transport_message, timeout);
        let pinned_future = Pin::from(future);
        let result = pinned_future.await;
        match (self.deserializer)(result) {
            Err(_e) => Err(StockTrekError::General(GeneralError::Message(
                "".to_string(),
            ))),
            Ok(message) => Ok(message),
        }
    }
}
