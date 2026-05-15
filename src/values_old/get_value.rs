use crate::destroy::Destroy;
use stock_trek::error::result::StockTrekResult;

pub trait GetValue<TValue, TState, TCredentials>
where
    TCredentials: Destroy,
{
    fn get(&self, state: &TState, credentials: &TCredentials) -> StockTrekResult<TValue>;
}
