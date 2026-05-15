use stock_trek::error::result::StockTrekResult;

pub type PackValue<TValue, TMessage>
where
    TValue: Clone,
= fn(message: &mut TMessage, value: &TValue) -> StockTrekResult<()>;
