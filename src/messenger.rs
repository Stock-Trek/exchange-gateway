use crate::{
    functions::{DeserializeReply, FilterReply, MessageToDto},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;
use std::sync::Arc;
use stock_trek::error::result::StockTrekResult;

pub type Messenger<TRequest, TResponse> = Box<dyn MessengerTrait<TRequest, TResponse>>;

#[async_trait]
pub trait MessengerTrait<TRequest, TResponse>: Send + Sync {
    async fn send(&self, request: TRequest) -> StockTrekResult<TResponse>;
}

pub struct MessengerImpl<TTransport, TRequest, TResponse, TFilteredResponse>
where
    TTransport: TransportTrait + ?Sized,
{
    transport: Arc<TTransport>,
    timeout: Duration,
    to_dto: MessageToDto<TRequest, TTransport::MessageDto>,
    deserialize_reply: DeserializeReply<TTransport::MessageDto, TResponse>,
    filter_reply: FilterReply<TResponse, TFilteredResponse>,
}

impl<TTransport, TRequest, TResponse, TFilteredResponse>
    MessengerImpl<TTransport, TRequest, TResponse, TFilteredResponse>
where
    TTransport: TransportTrait + ?Sized + 'static,
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
    TFilteredResponse: Send + Sync + 'static,
{
    pub fn new(
        transport: Arc<TTransport>,
        timeout: Duration,
        to_dto: MessageToDto<TRequest, TTransport::MessageDto>,
        deserialize_reply: DeserializeReply<TTransport::MessageDto, TResponse>,
        filter_reply: FilterReply<TResponse, TFilteredResponse>,
    ) -> Messenger<TRequest, TFilteredResponse> {
        Box::new(Self {
            transport,
            timeout,
            to_dto,
            deserialize_reply,
            filter_reply,
        })
    }
}

#[async_trait]
impl<TTransport, TRequest, TResponse, TFilteredResponse> MessengerTrait<TRequest, TFilteredResponse>
    for MessengerImpl<TTransport, TRequest, TResponse, TFilteredResponse>
where
    TTransport: TransportTrait + ?Sized,
    TRequest: Send,
{
    async fn send(&self, request: TRequest) -> StockTrekResult<TFilteredResponse> {
        let dto = (self.to_dto)(&request)?;
        let reply_dto = self.transport.send(dto, self.timeout).await?;
        let reply = (self.deserialize_reply)(reply_dto)?;
        (self.filter_reply)(reply)
    }
}
