use crate::values::pack_value::PackValue;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct PackMessage<TMessagePart, TMessage, TSetter, TValue>
where
    TSetter: Fn(&TMessagePart, &mut TMessage, TValue),
    TValue: Clone,
{
    setter: TSetter,
    part: TMessagePart,
    _phantom_message: PhantomData<TMessage>,
    _phantom_value: PhantomData<TValue>,
}

impl<TMessagePart, TMessage, TSetter, TValue> PackMessage<TMessagePart, TMessage, TSetter, TValue>
where
    TSetter: Fn(&TMessagePart, &mut TMessage, TValue),
    TValue: Clone,
{
    pub fn new(setter: TSetter, part: TMessagePart) -> Self {
        Self {
            setter,
            part,
            _phantom_message: PhantomData,
            _phantom_value: PhantomData,
        }
    }
}

impl<TMessagePart, TMessage, TSetter, TValue> PackValue<TValue, TMessagePart, TMessage>
    for PackMessage<TMessagePart, TMessage, TSetter, TValue>
where
    TSetter: Fn(&TMessagePart, &mut TMessage, TValue),
    TValue: Clone,
{
    fn pack(&self, message: &mut TMessage, value: &TValue) -> StockTrekResult<()> {
        Ok((self.setter)(&self.part, message, value.clone()))
    }
}
