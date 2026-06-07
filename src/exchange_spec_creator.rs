use crate::exchange_spec::ExchangeSpec;

pub type ExchangeSpecCreator<TTransports, TCredentials, TState> =
    Box<dyn ExchangeSpecCreatorTrait<TTransports, TCredentials, TState>>;

pub trait ExchangeSpecCreatorTrait<TTransports, TCredentials, TState>
where
    TState: Default,
{
    fn create_spec(&self) -> ExchangeSpec<TTransports, TCredentials, TState>;
}
