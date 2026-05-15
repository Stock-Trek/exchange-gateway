use crate::adapt::{adapter::Adapter, adapter_creator::AdapterCreator};
use std::collections::HashMap;
use stock_trek::exchange_id::ExchangeId;

pub struct AdapterFactory {
    creators: HashMap<ExchangeId, AdapterCreator>,
}

impl AdapterFactory {
    pub fn new() -> Self {
        Self {
            creators: HashMap::new(),
        }
    }
    pub fn add(&mut self, adapter_creator: AdapterCreator) -> &mut Self {
        let exchange_id = adapter_creator.exchange_id();
        self.creators.insert(exchange_id, adapter_creator);
        self
    }
    pub fn create_adapter(&self, exchange_id: ExchangeId) -> Option<Adapter> {
        self.creators.get(&exchange_id).map(|c| c.create_adapter())
    }
}
