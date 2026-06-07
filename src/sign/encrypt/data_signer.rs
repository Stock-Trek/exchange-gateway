use stock_trek::error::result::StockTrekResult;

pub type DataSigner = Box<dyn DataSignerTrait>;

pub trait DataSignerTrait: Send + Sync {
    fn sign(&self, data: &[u8]) -> StockTrekResult<Vec<u8>>;
}
