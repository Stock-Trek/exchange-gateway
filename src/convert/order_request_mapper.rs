use crate::convert::{
    single_order_field_mapper::SingleOrderFieldMapper,
    single_order_location::{
        OneCancelsOtherLocation, OneTriggersOcoLocation, OneTriggersOtherLocation,
        SingleOrderLocation,
    },
};
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use std::{collections::HashMap, rc::Rc};
use stock_trek::{
    asset_id::AssetId,
    order::{order_request::OrderRequest, orders::single::SingleOrderGeneric},
};

pub type OrderRequestMapper = Box<dyn OrderRequestMapperTrait>;

pub trait OrderRequestMapperTrait {
    fn map_order_request(&self, order_request: &OrderRequest<AssetId, Decimal>) -> Value;
}

pub struct OrderRequestMapping {
    field_mappers: HashMap<SingleOrderLocation, Vec<Rc<SingleOrderFieldMapper>>>,
}

impl OrderRequestMapperTrait for OrderRequestMapping {
    fn map_order_request(&self, order_request: &OrderRequest<AssetId, Decimal>) -> Value {
        let mut converted_order = Value::Object(Map::new());
        match order_request {
            OrderRequest::Single(single) => {
                self.map_fields(&mut converted_order, &single, SingleOrderLocation::Single);
            }
            OrderRequest::OneCancelsOther(oco) => {
                self.map_fields(
                    &mut converted_order,
                    &oco.primary,
                    SingleOrderLocation::OneCancelsOther(OneCancelsOtherLocation::Primary),
                );
                self.map_fields(
                    &mut converted_order,
                    &oco.secondary,
                    SingleOrderLocation::OneCancelsOther(OneCancelsOtherLocation::Secondary),
                );
            }
            OrderRequest::OneTriggersOther(oto) => {
                self.map_fields(
                    &mut converted_order,
                    &oto.primary,
                    SingleOrderLocation::OneTriggersOther(OneTriggersOtherLocation::Primary),
                );
                self.map_fields(
                    &mut converted_order,
                    &oto.secondary,
                    SingleOrderLocation::OneTriggersOther(OneTriggersOtherLocation::Secondary),
                );
            }
            OrderRequest::OneTriggersOco(otoco) => {
                self.map_fields(
                    &mut converted_order,
                    &otoco.primary,
                    SingleOrderLocation::OneTriggersOco(OneTriggersOcoLocation::Primary),
                );
                self.map_fields(
                    &mut converted_order,
                    &otoco.oco_order.primary,
                    SingleOrderLocation::OneTriggersOco(OneTriggersOcoLocation::OcoPrimary),
                );
                self.map_fields(
                    &mut converted_order,
                    &otoco.oco_order.secondary,
                    SingleOrderLocation::OneTriggersOco(OneTriggersOcoLocation::OcoSecondary),
                );
            }
        }
        converted_order
    }
}

impl OrderRequestMapping {
    pub fn add_field_mapper(
        &mut self,
        mapper: SingleOrderFieldMapper,
        locations: &[SingleOrderLocation],
    ) {
        let rc_mapper = Rc::new(mapper);
        for location in locations {
            self.field_mappers
                .entry(location.clone())
                .or_insert(Vec::new())
                .push(rc_mapper.clone());
        }
    }
    fn map_fields(
        &self,
        converted_order: &mut Value,
        single_order: &SingleOrderGeneric<AssetId, Decimal>,
        location: SingleOrderLocation,
    ) {
        let mappers_opt = self.field_mappers.get(&location);
        if let Some(mappers) = mappers_opt {
            for mapper in mappers {
                mapper.map_value(converted_order, single_order);
            }
        }
    }
}
