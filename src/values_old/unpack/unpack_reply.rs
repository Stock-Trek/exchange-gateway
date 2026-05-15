use crate::values::unpack_value::UnpackValue;
use stock_trek::error::result::StockTrekResult;

pub struct UnpackReply<TReply, TValue> {
    getter: fn(&TReply) -> TValue,
}

impl<TReply, TValue> UnpackReply<TReply, TValue> {
    pub fn new(getter: fn(&TReply) -> TValue) -> Self {
        Self { getter }
    }
}

impl<TReply, TValue> UnpackValue<TValue, TReply> for UnpackReply<TReply, TValue> {
    fn unpack(&self, reply: &TReply) -> StockTrekResult<TValue> {
        Ok((self.getter)(reply))
    }
}
