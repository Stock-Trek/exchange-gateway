use crate::{
    authentication_state::{Authenticated, AuthenticationState, Scratch, Unauthenticated},
    credentials::Credential,
    exchange_spec::ExchangeSpec,
};
use std::marker::PhantomData;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

pub struct ExchangeConnector<TTransports, TState, TTradeRequest, TTradeResponse, TAuthState>
where
    TState: Default,
    TAuthState: AuthenticationState,
{
    spec: ExchangeSpec<TTransports, TState, TTradeRequest, TTradeResponse>,
    transports: TTransports,
    credentials: Box<dyn Credential>,
    state: TState,
    _phantom_auth_state: PhantomData<TAuthState>,
}

impl<TTransports, TState, TTradeRequest, TTradeResponse>
    ExchangeConnector<TTransports, TState, TTradeRequest, TTradeResponse, Scratch>
where
    TTransports: Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub fn new(
        spec: ExchangeSpec<TTransports, TState, TTradeRequest, TTradeResponse>,
        transports: TTransports,
        credentials: Box<dyn Credential>,
    ) -> ExchangeConnector<TTransports, TState, TTradeRequest, TTradeResponse, Unauthenticated>
    {
        ExchangeConnector::<TTransports, TState, TTradeRequest, TTradeResponse, Unauthenticated> {
            spec,
            transports,
            credentials,
            state: TState::default(),
            _phantom_auth_state: PhantomData,
        }
    }
}

impl<TTransports, TState, TTradeRequest, TTradeResponse>
    ExchangeConnector<TTransports, TState, TTradeRequest, TTradeResponse, Unauthenticated>
where
    TTransports: Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub async fn authenticate(
        self,
    ) -> StockTrekResult<
        ExchangeConnector<TTransports, TState, TTradeRequest, TTradeResponse, Authenticated>,
    > {
        let state = self
            .spec
            .authenticate(&self.transports, &*self.credentials)
            .await?;
        Ok(
            ExchangeConnector::<TTransports, TState, TTradeRequest, TTradeResponse, Authenticated> {
                spec: self.spec,
                transports: self.transports,
                credentials: self.credentials,
                state,
                _phantom_auth_state: PhantomData,
            },
        )
    }
}

impl<TTransports, TState, TTradeRequest, TTradeResponse>
    ExchangeConnector<TTransports, TState, TTradeRequest, TTradeResponse, Authenticated>
where
    TTransports: Send + Sync,
    TState: Default + Send + Sync,
{
    pub async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TTradeRequest,
    ) -> StockTrekResult<TTradeResponse> {
        self.spec
            .send_trade_request(
                &self.transports,
                &*self.credentials,
                &self.state,
                preferences,
                trade_request,
            )
            .await
    }
}
