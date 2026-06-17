use crate::{cex::increment_sizes::IncrementSizes, functions::ToIncrements, messenger::Messenger};
use async_trait::async_trait;
use std::collections::HashMap;
use stock_trek::{cex::trading_pair::TradingPair, error::result::StockTrekResult};

pub type IncrementsLeg = Box<dyn IncrementsLegTrait>;

#[async_trait]
pub trait IncrementsLegTrait: Send + Sync {
    async fn get_increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>>;
}

pub struct IncrementsLegImpl<TMessage, TIncrements> {
    message: TMessage,
    messenger: Messenger<TMessage, TIncrements>,
    to_increments: ToIncrements<TIncrements>,
}

impl<TMessage, TIncrements> IncrementsLegImpl<TMessage, TIncrements>
where
    TMessage: Clone + Send + Sync + 'static,
    TIncrements: Send + Sync + 'static,
{
    pub fn new(
        message: TMessage,
        messenger: Messenger<TMessage, TIncrements>,
        to_increments: ToIncrements<TIncrements>,
    ) -> IncrementsLeg {
        Box::new(Self {
            message,
            messenger,
            to_increments,
        })
    }
}

#[async_trait]
impl<TMessage, TIncrements> IncrementsLegTrait for IncrementsLegImpl<TMessage, TIncrements>
where
    TMessage: Clone + Send + Sync,
    TIncrements: Send + Sync,
{
    async fn get_increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>> {
        let increments = self.messenger.send(self.message.clone()).await?;
        Ok((self.to_increments)(increments))
    }
}
