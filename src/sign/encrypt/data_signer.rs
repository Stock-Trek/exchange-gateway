use crate::error::EGResult;
use std::sync::Arc;

pub(crate) type DataSigner = Arc<dyn DataSignerTrait>;

pub(crate) trait DataSignerTrait: Send + Sync {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>>;
}
