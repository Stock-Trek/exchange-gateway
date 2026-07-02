use crate::{
    error::EGResult,
    functions::{TryConvertRef, TryConvertValue},
    listeners::listener::{Listener, ListenerTrait},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;

pub type Messenger<TMessageToExchange, TMessageFromExchange> =
    Box<dyn MessengerTrait<TMessageToExchange, TMessageFromExchange>>;

#[async_trait]
pub trait MessengerTrait<TMessageToExchange, TMessageFromExchange>: Send + Sync {
    async fn send(&self, request: &TMessageToExchange, timeout: Duration) -> EGResult<()>;
}

pub struct MessengerImpl<TTransport, TMessageToExchange, TMessageFromExchange>
where
    TTransport: TransportTrait,
{
    transport: TTransport,
    listener: Listener<TMessageFromExchange>,
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
        listener: Listener<TMessageFromExchange>,
        to_dto: TryConvertRef<TMessageToExchange, TTransport::MessageDto>,
        from_dto: TryConvertValue<TTransport::MessageDto, TMessageFromExchange>,
    ) -> Self {
        Self {
            transport,
            listener,
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
    async fn send(&self, request: &TMessageToExchange, timeout: Duration) -> EGResult<()> {
        let request_dto = (self.to_dto)(request)?;
        self.transport.send(request_dto, timeout).await
    }
}

#[async_trait]
impl<TTransport, TMessageToExchange, TMessageFromExchange> ListenerTrait<TTransport::MessageDto>
    for MessengerImpl<TTransport, TMessageToExchange, TMessageFromExchange>
where
    TTransport: TransportTrait,
{
    async fn on_message(&self, message: TTransport::MessageDto) -> EGResult<()> {
        let reply_dto = (self.from_dto)(message)?;
        self.listener.on_message(reply_dto).await
    }
}
