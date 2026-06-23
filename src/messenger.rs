use crate::{
    error::EGResult,
    functions::{TryConvertRef, TryConvertValue},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;

pub type Messenger<TMessageToExchange, TMessageFromExchange> =
    Box<dyn MessengerTrait<TMessageToExchange, TMessageFromExchange>>;

#[async_trait]
pub trait MessengerTrait<TMessageToExchange, TMessageFromExchange>: Send + Sync {
    async fn send(
        &self,
        request: &TMessageToExchange,
        timeout: Duration,
    ) -> EGResult<TMessageFromExchange>;
}

pub struct MessengerImpl<TTransport, TMessageToExchange, TMessageFromExchange>
where
    TTransport: TransportTrait,
{
    transport: TTransport,
    to_dto: TryConvertRef<TMessageToExchange, TTransport::MessageDto>,
    from_dto: TryConvertValue<TTransport::MessageDto, TMessageFromExchange>,
}

impl<TTransport, TMessageToExchange, TMessageFromExchange>
    MessengerImpl<TTransport, TMessageToExchange, TMessageFromExchange>
where
    TTransport: TransportTrait + 'static,
    TMessageToExchange: Send + Sync + 'static,
    TMessageFromExchange: Send + Sync + 'static,
{
    pub fn new(
        transport: TTransport,
        to_dto: TryConvertRef<TMessageToExchange, TTransport::MessageDto>,
        from_dto: TryConvertValue<TTransport::MessageDto, TMessageFromExchange>,
    ) -> Self {
        Self {
            transport,
            to_dto,
            from_dto,
        }
    }
}

#[async_trait]
impl<TTransport, TMessageToExchange, TMessageFromExchange>
    MessengerTrait<TMessageToExchange, TMessageFromExchange>
    for MessengerImpl<TTransport, TMessageToExchange, TMessageFromExchange>
where
    TTransport: TransportTrait,
    TMessageToExchange: Send + Sync,
{
    async fn send(
        &self,
        request: &TMessageToExchange,
        timeout: Duration,
    ) -> EGResult<TMessageFromExchange> {
        let request_dto = (self.to_dto)(request)?;
        let reply_dto = self.transport.send(request_dto, timeout).await?;
        (self.from_dto)(reply_dto)
    }
}
