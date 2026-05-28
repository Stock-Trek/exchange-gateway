use crate::{destroy::Destroy, exchange_spec::ExchangeSpec};

pub type ExchangeSpecCreator<TTransports, TCredentials, TState> =
    Box<dyn ExchangeSpecCreatorTrait<TTransports, TCredentials, TState>>;

pub trait ExchangeSpecCreatorTrait<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    fn create_spec(&self) -> ExchangeSpec<TTransports, TCredentials, TState>;
}
