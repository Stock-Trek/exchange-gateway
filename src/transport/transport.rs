use async_trait::async_trait;
use chrono::Duration;
use std::{marker::PhantomData, pin::Pin};
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

#[async_trait]
pub trait TransportTrait
where
    Self::TransportMessage: From<Self::MessageDto>,
    Self::MessageDto: TryFrom<Self::TransportMessage>,
{
    type MessageDto;
    type TransportMessage;
    async fn send(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
    ) -> StockTrekResult<Self::MessageDto>;
}

pub struct TransportImpl<TransportMessage, MessageDto>
where
    TransportMessage: From<MessageDto>,
    MessageDto: TryFrom<TransportMessage>,
{
    delegate: fn(TransportMessage, Duration) -> Box<dyn Future<Output = TransportMessage> + Send>,
    _phantom_message_dto: PhantomData<MessageDto>,
}

impl<TransportMessage, MessageDto> TransportImpl<TransportMessage, MessageDto>
where
    TransportMessage: From<MessageDto>,
    MessageDto: TryFrom<TransportMessage>,
{
    pub fn new(
        delegate: fn(
            TransportMessage,
            Duration,
        ) -> Box<dyn Future<Output = TransportMessage> + Send>,
    ) -> Self {
        Self {
            delegate,
            _phantom_message_dto: PhantomData,
        }
    }
}

#[async_trait]
impl<TransportMessage, MessageDto> TransportTrait for TransportImpl<TransportMessage, MessageDto>
where
    TransportMessage: From<MessageDto>,
    MessageDto: TryFrom<TransportMessage> + Send + Sync,
{
    type TransportMessage = TransportMessage;
    type MessageDto = MessageDto;
    async fn send(
        &self,
        message_dto: MessageDto,
        timeout: Duration,
    ) -> StockTrekResult<MessageDto> {
        let transport_message = message_dto.into();
        let future = (self.delegate)(transport_message, timeout);
        let pinned_future = Pin::from(future);
        let result = pinned_future.await;
        match result.try_into() {
            Err(_e) => Err(StockTrekError::General(GeneralError::Message(
                "".to_string(),
            ))),
            Ok(message) => Ok(message),
        }
    }
}
