use stock_trek::order::{order_id::OrderId, order_response::OrderResponse};

pub struct OrderResponseExtractor<TReply> {
    pub id: OrderResponseFieldExtractor<TReply, String>,
}

impl<TReply> OrderResponseExtractor<TReply> {
    pub fn new(id: OrderResponseFieldExtractor<TReply, String>) -> Self {
        Self { id }
    }
    pub fn extract(&self, reply: &TReply) -> OrderResponse {
        let id = OrderId((self.id)(reply));
        OrderResponse { id }
    }
}

pub type OrderResponseFieldExtractor<TReply, TValue> = fn(&TReply) -> TValue;
