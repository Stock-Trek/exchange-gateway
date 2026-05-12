use stock_trek::error::result::StockTrekResult;

pub trait UnpackValue<TValue, TReply> {
    fn unpack(&self, reply: &TReply) -> StockTrekResult<TValue>;
}
