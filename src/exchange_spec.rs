use async_trait::async_trait;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

pub type ExchangeSpec<TCredentials, TState, TTradeRequest, TTradeResponse> =
    Box<dyn ExchangeSpecTrait<TCredentials, TState, TTradeRequest, TTradeResponse>>;

#[async_trait]
pub trait ExchangeSpecTrait<TCredentials, TState, TTradeRequest, TTradeResponse>
where
    TState: Default,
{
    async fn authenticate(&self, credentials: &TCredentials) -> StockTrekResult<TState>;
    async fn send_trade_request(
        &self,
        credentials: &TCredentials,
        state: &TState,
        preferences: &Preferences,
        trade_request: TTradeRequest,
    ) -> StockTrekResult<TTradeResponse>;
}
