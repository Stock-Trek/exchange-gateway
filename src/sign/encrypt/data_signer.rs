use std::sync::Arc;
use stock_trek::error::result::StockTrekResult;

pub type DataSigner = Arc<dyn DataSignerTrait>;

pub trait DataSignerTrait: Send + Sync {
    fn sign(&self, data: &[u8]) -> StockTrekResult<Vec<u8>>;
}
