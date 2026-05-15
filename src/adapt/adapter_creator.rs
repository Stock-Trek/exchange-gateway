use crate::adapt::adapter::Adapter;
use stock_trek::exchange_id::ExchangeId;

pub type AdapterCreator = Box<dyn AdapterCreatorTrait>;

pub trait AdapterCreatorTrait {
    fn exchange_id(&self) -> ExchangeId;
    fn create_adapter(&self) -> Adapter;
}
