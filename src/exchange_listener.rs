use stock_trek::order::order_response::OrderResponse;

pub type ExchangeListener = Box<dyn ExchangeListenerTrait>;

pub trait ExchangeListenerTrait: Send + Sync {
    fn on_order_placed(&self, order_response: OrderResponse);
}
