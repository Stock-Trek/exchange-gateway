use stock_trek::order::{order_id::OrderId, order_response::OrderResponse};

pub type FieldUnmarshaller<TReply, TValue> = fn(&TReply) -> TValue;

pub struct OrderResponseUnmarshaller<TReply> {
    pub id: FieldUnmarshaller<TReply, String>,
}

impl<TReply> OrderResponseUnmarshaller<TReply> {
    pub fn unmarshall(&self, reply: &TReply) -> OrderResponse {
        let id = OrderId((self.id)(reply));
        OrderResponse { id }
    }
}
