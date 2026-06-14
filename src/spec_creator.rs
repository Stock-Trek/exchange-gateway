use crate::exchange_spec::ExchangeSpec;

pub type SpecCreator<TTransports, TState, TTradeRequest, TTradeResponse> =
    Box<dyn SpecCreatorTrait<TTransports, TState, TTradeRequest, TTradeResponse>>;

pub trait SpecCreatorTrait<TTransports, TState, TTradeRequest, TTradeResponse>
where
    TState: Default,
{
    fn create_spec(&self) -> ExchangeSpec<TTransports, TState, TTradeRequest, TTradeResponse>;
}
