use crate::{
    cex::increment_sizes::IncrementSizes,
    functions::{DeserializeReply, FilterReply, MessageToDto, ToIncrements},
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;
use std::{collections::HashMap, sync::Arc};
use stock_trek::{cex::trading_pair::TradingPair, error::result::StockTrekResult};

pub type IncrementsLeg = Box<dyn IncrementsLegTrait>;

#[async_trait]
pub trait IncrementsLegTrait: Send + Sync {
    async fn get_increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>>;
}

pub struct IncrementsLegImpl<TTransport, TMessage, TRawReply, TIncrements>
where
    TTransport: TransportTrait + ?Sized,
{
    transport: Arc<TTransport>,
    timeout: Duration,
    message: TMessage,
    to_dto: MessageToDto<TMessage, TTransport::MessageDto>,
    deserialize_reply: DeserializeReply<TTransport::MessageDto, TRawReply>,
    filter_reply: FilterReply<TRawReply, TIncrements>,
    to_increments: ToIncrements<TIncrements>,
}

impl<TTransport, TMessage, TRawReply, TIncrements>
    IncrementsLegImpl<TTransport, TMessage, TRawReply, TIncrements>
where
    TTransport: TransportTrait + Sync + ?Sized + 'static,
    TMessage: Send + Sync + 'static,
    TRawReply: Send + Sync + 'static,
    TIncrements: Send + Sync + 'static,
{
    pub fn new(
        transport: Arc<TTransport>,
        timeout: Duration,
        message: TMessage,
        to_dto: MessageToDto<TMessage, TTransport::MessageDto>,
        deserialize_reply: DeserializeReply<TTransport::MessageDto, TRawReply>,
        filter_reply: FilterReply<TRawReply, TIncrements>,
        to_increments: ToIncrements<TIncrements>,
    ) -> IncrementsLeg {
        Box::new(Self {
            transport,
            timeout,
            message,
            to_dto,
            deserialize_reply,
            filter_reply,
            to_increments,
        })
    }
}

#[async_trait]
impl<TTransport, TMessage, TRawReply, TIncrements> IncrementsLegTrait
    for IncrementsLegImpl<TTransport, TMessage, TRawReply, TIncrements>
where
    TTransport: TransportTrait + Send + Sync + ?Sized,
    TMessage: Send + Sync,
    TRawReply: Send + Sync,
    TIncrements: Send + Sync,
{
    async fn get_increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>> {
        let dto = (self.to_dto)(&self.message)?;
        let reply = self.transport.send(dto, self.timeout).await?;
        let deserialized_reply = (self.deserialize_reply)(reply)?;
        let reply = (self.filter_reply)(deserialized_reply)?;
        Ok((self.to_increments)(reply))
    }
}
