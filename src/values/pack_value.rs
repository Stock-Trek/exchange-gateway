use stock_trek::error::result::StockTrekResult;

pub trait PackValue<TValue, TMessagePart, TMessage>
where
    TValue: Clone,
{
    fn pack(&self, message: &mut TMessage, value: &TValue) -> StockTrekResult<()>;
}
