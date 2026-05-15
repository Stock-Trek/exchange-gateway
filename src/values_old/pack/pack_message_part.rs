use crate::values::pack_value::PackValue;
use stock_trek::error::result::StockTrekResult;

pub struct PackMessage<TMessage, TValue>
where
    TValue: Clone,
{
    setter: fn(&mut TMessage, TValue),
}

impl<TMessage, TValue> PackMessage<TMessage, TValue>
where
    TValue: Clone,
{
    pub fn new(setter: fn(&mut TMessage, TValue)) -> Self {
        Self { setter, part }
    }
}

impl<TMessage, TValue> PackValue<TValue, TMessage> for PackMessage<TMessage, TValue>
where
    TValue: Clone,
{
    fn pack(&self, message: &mut TMessage, value: &TValue) -> StockTrekResult<()> {
        Ok((self.setter)(&self.part, message, value.clone()))
    }
}
