use stock_trek::error::result::StockTrekResult;

pub type UnpackValue<TValue, TReply> = fn(reply: &TReply) -> StockTrekResult<TValue>;
