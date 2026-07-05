use crate::error::EGResult;

pub(crate) type DataSigner = Box<dyn DataSignerTrait>;

pub(crate) trait DataSignerTrait: Send + Sync {
    fn sign(&self, data: &[u8]) -> EGResult<Vec<u8>>;
}
