use crate::exchange_spec::ExchangeSpec;

pub type SpecCreator<TCredentials, TState, TTradeRequest, TTradeResponse> =
    Box<dyn SpecCreatorTrait<TCredentials, TState, TTradeRequest, TTradeResponse>>;

pub trait SpecCreatorTrait<TCredentials, TState, TTradeRequest, TTradeResponse>
where
    TState: Default,
{
    fn create_spec(&self) -> ExchangeSpec<TCredentials, TState, TTradeRequest, TTradeResponse>;
}
