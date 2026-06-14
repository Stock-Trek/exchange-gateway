use async_trait::async_trait;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

use crate::credentials::Credential;

pub type ExchangeSpec<TTransports, TState, TTradeRequest, TTradeResponse> =
    Box<dyn ExchangeSpecTrait<TTransports, TState, TTradeRequest, TTradeResponse>>;

#[async_trait]
pub trait ExchangeSpecTrait<TTransports, TState, TTradeRequest, TTradeResponse>
where
    TState: Default,
{
    async fn authenticate(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
    ) -> StockTrekResult<TState>;
    async fn send_trade_request(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
        state: &TState,
        preferences: &Preferences,
        trade_request: TTradeRequest,
    ) -> StockTrekResult<TTradeResponse>;
}
