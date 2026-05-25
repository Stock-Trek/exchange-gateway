pub trait ExtractorTrait<TFrom, TTo>: Send + Sync {
    fn extract(&self, order: &TFrom) -> TTo;
}
