use crate::error::EGResult;
use std::sync::Arc;

pub type DataSigner = Arc<dyn DataSignerTrait>;

pub trait DataSignerTrait: Send + Sync {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>>;
}
