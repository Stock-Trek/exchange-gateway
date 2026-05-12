use crate::values::unpack_value::UnpackValue;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct UnpackReply<TReply, TGetter, TValue>
where
    TGetter: Fn(&TReply) -> TValue,
{
    getter: TGetter,
    _phantom_reply: PhantomData<TReply>,
    _phantom_value: PhantomData<TValue>,
}

impl<TReply, TGetter, TValue> UnpackReply<TReply, TGetter, TValue>
where
    TGetter: Fn(&TReply) -> TValue,
{
    pub fn new(getter: TGetter) -> Self {
        Self {
            getter,
            _phantom_reply: PhantomData,
            _phantom_value: PhantomData,
        }
    }
}

impl<TReply, TGetter, TValue> UnpackValue<TValue, TReply> for UnpackReply<TReply, TGetter, TValue>
where
    TGetter: Fn(&TReply) -> TValue,
{
    fn unpack(&self, reply: &TReply) -> StockTrekResult<TValue> {
        Ok((self.getter)(reply))
    }
}
