use rust_decimal::Decimal;
use stock_trek::{asset_id::AssetId, order::orders::single::SingleOrderGeneric};

pub type SingleOrderFieldMarshaller<TMessage> = Box<dyn SingleOrderFieldMarshallerTrait<TMessage>>;

pub trait SingleOrderFieldMarshallerTrait<TMessage>: Send + Sync {
    fn marshall(&self, single_order: &SingleOrderGeneric<AssetId, Decimal>, message: &mut TMessage);
}

pub struct SingleOrderFieldMarshalling<TMessage, TValue> {
    pub getter: Box<dyn Fn(&SingleOrderGeneric<AssetId, Decimal>) -> TValue + Send + Sync>,
    pub setter: Box<dyn Fn(TValue, &mut TMessage) + Send + Sync>,
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
