use async_trait::async_trait;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

pub type ExchangeSpec<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse> =
    Box<dyn ExchangeSpecTrait<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse>>;

#[async_trait]
pub trait ExchangeSpecTrait<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse>
where
    TState: Default,
{
    async fn authenticate(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
    ) -> StockTrekResult<TState>;
    async fn send_trade_request(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &TState,
        preferences: &Preferences,
        trade_request: TTradeRequest,
    ) -> StockTrekResult<TTradeResponse>;
}
