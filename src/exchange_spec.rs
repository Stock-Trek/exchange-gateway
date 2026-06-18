use crate::{cex::increment_sizes::IncrementSizes, sign::signer::Signer};
use async_trait::async_trait;
use std::collections::HashMap;
use stock_trek::{
    cex::trading_pair::TradingPair, error::result::StockTrekResult, preferences::Preferences,
};

pub type ExchangeSpec<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> =
    Box<dyn ExchangeSpecTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>>;

#[async_trait]
pub trait ExchangeSpecTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>:
    Send + Sync
{
    async fn increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>>;
    async fn authenticate(
        &self,
        initial_auth_leg_signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>>;
    async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TTradeRequest,
        increments: &HashMap<TradingPair, IncrementSizes>,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<TTradeResponse>;
}
