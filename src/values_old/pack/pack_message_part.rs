use crate::values::pack_value::PackValue;
use stock_trek::error::result::StockTrekResult;

pub struct PackMessage<TMessagePart, TMessage, TValue>
where
    TValue: Clone,
{
    setter: fn(&TMessagePart, &mut TMessage, TValue),
    part: TMessagePart,
}

impl<TMessagePart, TMessage, TValue> PackMessage<TMessagePart, TMessage, TValue>
where
    TValue: Clone,
{
    pub fn new(setter: fn(&TMessagePart, &mut TMessage, TValue), part: TMessagePart) -> Self {
        Self { setter, part }
    }
}

impl<TMessagePart, TMessage, TValue> PackValue<TValue, TMessagePart, TMessage>
    for PackMessage<TMessagePart, TMessage, TValue>
where
    TValue: Clone,
{
    fn pack(&self, message: &mut TMessage, value: &TValue) -> StockTrekResult<()> {
        Ok((self.setter)(&self.part, message, value.clone()))
    }
}
