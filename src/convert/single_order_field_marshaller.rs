use rust_decimal::Decimal;
use stock_trek::{asset_id::AssetId, order::orders::single::SingleOrderGeneric};

pub type SingleOrderFieldMarshaller<TMessage> = Box<dyn SingleOrderFieldMarshallerTrait<TMessage>>;

pub trait SingleOrderFieldMarshallerTrait<TMessage> {
    fn marshall(&self, single_order: &SingleOrderGeneric<AssetId, Decimal>, message: &mut TMessage);
}

pub struct SingleOrderFieldMarshalling<TMessage, TValue> {
    pub getter: fn(&SingleOrderGeneric<AssetId, Decimal>) -> TValue,
    pub setter: fn(TValue, &mut TMessage),
}

impl<TMessage, TValue> SingleOrderFieldMarshallerTrait<TMessage>
    for SingleOrderFieldMarshalling<TMessage, TValue>
{
    fn marshall(
        &self,
        single_order: &SingleOrderGeneric<AssetId, Decimal>,
        message: &mut TMessage,
    ) {
        let value = (self.getter)(single_order);
        (self.setter)(value, message);
    }
}
