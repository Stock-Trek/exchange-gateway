use crate::convert::{
    order_response_unmarshaller::OrderResponseUnmarshaller,
    single_order_field_marshaller::SingleOrderFieldMarshaller,
    single_order_location::{
        OneCancelsOtherLocation, OneTriggersOcoLocation, OneTriggersOtherLocation,
        SingleOrderLocation,
    },
};
use rust_decimal::Decimal;
use std::{collections::HashMap, rc::Rc};
use stock_trek::{
    asset_id::AssetId,
    order::{
        order_request::OrderRequest, order_response::OrderResponse,
        orders::single::SingleOrderGeneric,
    },
};

pub type OrderRequestMarshallUnmarshaller<TMessage, TReply> =
    Box<dyn OrderRequestMarshallUnmarshallerTrait<TMessage, TReply>>;

pub trait OrderRequestMarshallUnmarshallerTrait<TMessage, TReply>: Send + Sync
where
    TMessage: Default,
{
    fn marshall(&self, order_request: &OrderRequest<AssetId, Decimal>) -> TMessage;
    fn unmarshall(&self, reply: &TReply) -> OrderResponse;
}

pub struct OrderRequestMapping<TMessage, TReply>
where
    TMessage: Default,
{
    marshallers: HashMap<SingleOrderLocation, Vec<Rc<SingleOrderFieldMarshaller<TMessage>>>>,
    unmarshaller: OrderResponseUnmarshaller<TReply>,
}

impl<TMessage, TReply> OrderRequestMarshallUnmarshallerTrait<TMessage, TReply>
    for OrderRequestMapping<TMessage, TReply>
where
    TMessage: Default,
{
    fn marshall(&self, order_request: &OrderRequest<AssetId, Decimal>) -> TMessage {
        let mut message = TMessage::default();
        match order_request {
            OrderRequest::Single(single) => {
                self.marshall_single_order(&single, SingleOrderLocation::Single, &mut message);
            }
            OrderRequest::OneCancelsOther(oco) => {
                self.marshall_single_order(
                    &oco.primary,
                    SingleOrderLocation::OneCancelsOther(OneCancelsOtherLocation::Primary),
                    &mut message,
                );
                self.marshall_single_order(
                    &oco.secondary,
                    SingleOrderLocation::OneCancelsOther(OneCancelsOtherLocation::Secondary),
                    &mut message,
                );
            }
            OrderRequest::OneTriggersOther(oto) => {
                self.marshall_single_order(
                    &oto.primary,
                    SingleOrderLocation::OneTriggersOther(OneTriggersOtherLocation::Primary),
                    &mut message,
                );
                self.marshall_single_order(
                    &oto.secondary,
                    SingleOrderLocation::OneTriggersOther(OneTriggersOtherLocation::Secondary),
                    &mut message,
                );
            }
            OrderRequest::OneTriggersOco(otoco) => {
                self.marshall_single_order(
                    &otoco.primary,
                    SingleOrderLocation::OneTriggersOco(OneTriggersOcoLocation::Primary),
                    &mut message,
                );
                self.marshall_single_order(
                    &otoco.oco_order.primary,
                    SingleOrderLocation::OneTriggersOco(OneTriggersOcoLocation::OcoPrimary),
                    &mut message,
                );
                self.marshall_single_order(
                    &otoco.oco_order.secondary,
                    SingleOrderLocation::OneTriggersOco(OneTriggersOcoLocation::OcoSecondary),
                    &mut message,
                );
            }
        }
        message
    }
    fn unmarshall(&self, reply: &TReply) -> OrderResponse {
        self.unmarshaller.unmarshall(reply)
    }
}

impl<TMessage, TReply> OrderRequestMapping<TMessage, TReply>
where
    TMessage: Default,
{
    pub fn new(unmarshaller: OrderResponseUnmarshaller<TReply>) -> Self {
        Self {
            marshallers: HashMap::new(),
            unmarshaller,
        }
    }
    pub fn add_marshaller(
        &mut self,
        marshaller: SingleOrderFieldMarshaller<TMessage>,
        locations: &[SingleOrderLocation],
    ) {
        let rc_mapper = Rc::new(marshaller);
        for location in locations {
            self.marshallers
                .entry(location.clone())
                .or_insert(Vec::new())
                .push(rc_mapper.clone());
        }
    }
    fn marshall_single_order(
        &self,
        single_order: &SingleOrderGeneric<AssetId, Decimal>,
        location: SingleOrderLocation,
        message: &mut TMessage,
    ) {
        let marshallers_opt = self.marshallers.get(&location);
        if let Some(marshallers) = marshallers_opt {
            for marshaller in marshallers {
                marshaller.marshall(single_order, message);
            }
        }
    }
}
