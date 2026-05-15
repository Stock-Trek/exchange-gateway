use stock_trek::error::result::StockTrekResult;

pub type GetValue<TValue, TState, TCredentials> =
    fn(state: &TState, credentials: &TCredentials) -> StockTrekResult<TValue>;
