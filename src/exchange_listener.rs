use stock_trek::order::order_response::OrderResponse;

pub trait ExchangeListener {
    fn on_order_placed(&self, order_response: OrderResponse);
}
