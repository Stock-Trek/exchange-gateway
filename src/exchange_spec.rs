use crate::{cex::increment_sizes::IncrementSizes, sign::signer::Signer};
use async_trait::async_trait;
use std::collections::HashMap;
use stock_trek::{
    cex::trading_pair::TradingPair, error::result::StockTrekResult, preferences::Preferences,
};

pub type ExchangeSpec<TRequest, TUnsignedMessage, TSignedMessage, TResponse> =
    Box<dyn ExchangeSpecTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>>;

#[async_trait]
pub trait ExchangeSpecTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>:
    Send + Sync
{
    async fn increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>>;
    async fn authenticate(
        &self,
        initial_auth_leg_signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>>;
    async fn send(
        &self,
        request: TRequest,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
        preferences: &Preferences,
        increments: &HashMap<TradingPair, IncrementSizes>,
    ) -> StockTrekResult<TResponse>;
}
