use rust_decimal::Decimal;
use serde_json::Value;
use stock_trek::{asset_id::AssetId, order::orders::single::SingleOrderGeneric};

pub type SingleOrderFieldMapper = Box<dyn SingleOrderFieldMapperTrait>;

pub trait SingleOrderFieldMapperTrait {
    fn map_value(
        &self,
        converted_order: &mut Value,
        single_order: &SingleOrderGeneric<AssetId, Decimal>,
    );
}

pub struct SingleOrderFieldMapping<TValue> {
    getter: fn(&SingleOrderGeneric<AssetId, Decimal>) -> TValue,
    setter: fn(&mut Value, TValue),
}

impl<TValue> SingleOrderFieldMapperTrait for SingleOrderFieldMapping<TValue> {
    fn map_value(
        &self,
        converted_order: &mut Value,
        single_order: &SingleOrderGeneric<AssetId, Decimal>,
    ) {
        let value = (self.getter)(single_order);
        (self.setter)(converted_order, value);
    }
}
