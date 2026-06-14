use async_trait::async_trait;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

#[async_trait]
pub trait ExchangeSpecTrait: Send + Sync {
    type Transports: Send + Sync;
    type Credentials: Send + Sync;
    type State: Default + Send + Sync;
    type TradeRequest: Send + Sync;
    type TradeResponse: Send;

    async fn authenticate(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
    ) -> StockTrekResult<Self::State>;
    async fn send_trade_request(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
        state: &Self::State,
        preferences: &Preferences,
        trade_request: Self::TradeRequest,
    ) -> StockTrekResult<Self::TradeResponse>;
}
