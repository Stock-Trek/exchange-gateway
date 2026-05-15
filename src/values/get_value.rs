use crate::destroy::Destroy;
use stock_trek::error::result::StockTrekResult;

pub type GetValue<TValue, TState, TCredentials>
where
    TCredentials: Destroy,
= fn(state: &TState, credentials: &TCredentials) -> StockTrekResult<TValue>;
