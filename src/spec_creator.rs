use crate::exchange_spec::ExchangeSpec;

pub type SpecCreator<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse> =
    Box<dyn SpecCreatorTrait<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse>>;

pub trait SpecCreatorTrait<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse>
where
    TState: Default,
{
    fn create_spec(
        &self,
    ) -> ExchangeSpec<TTransports, TCredentials, TState, TTradeRequest, TTradeResponse>;
}
