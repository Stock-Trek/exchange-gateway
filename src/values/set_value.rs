use stock_trek::error::result::StockTrekResult;

pub trait SetValue<TValue, TState>
where
    TValue: Clone,
{
    fn set(&self, state: &mut TState, value: &TValue) -> StockTrekResult<()>;
}
