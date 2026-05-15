use stock_trek::error::result::StockTrekResult;

pub type SetValue<TValue, TState>
where
    TValue: Clone,
= fn(state: &mut TState, value: &TValue) -> StockTrekResult<()>;
