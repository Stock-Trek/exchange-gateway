use stock_trek::error::result::StockTrekResult;

pub type PackValue<TValue, TMessage> =
    fn(message: &mut TMessage, value: &TValue) -> StockTrekResult<()>;
