use stock_trek::error::result::StockTrekResult;

pub type SetValue<TValue, TState> = fn(state: &mut TState, value: &TValue) -> StockTrekResult<()>;
