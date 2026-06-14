use crate::exchange_spec::ExchangeSpecTrait;

pub trait SpecCreatorTrait: Send + Sync {
    type Spec: ExchangeSpecTrait;

    fn create_spec(&self) -> Self::Spec;
}
