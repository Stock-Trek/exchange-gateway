use rust_decimal::Decimal;
use stock_trek::order::order_request::OrderRequest;

pub trait ExchangeClient: Sized {
    fn send_order_request(&self, order: OrderRequest<AssetId, Decimal>);
    fn on_order_accepted(&self, order: Order);
}
