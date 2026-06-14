use crate::{
    authentication_state::{Authenticated, AuthenticationState, Scratch, Unauthenticated},
    exchange_spec::ExchangeSpec,
};
use std::marker::PhantomData;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

pub struct ExchangeConnector<TCredentials, TState, TTradeRequest, TTradeResponse, TAuthState>
where
    TState: Default,
    TAuthState: AuthenticationState,
{
    spec: ExchangeSpec<TCredentials, TState, TTradeRequest, TTradeResponse>,
    credentials: TCredentials,
    state: TState,
    _phantom_auth_state: PhantomData<TAuthState>,
}

impl<TCredentials, TState, TTradeRequest, TTradeResponse>
    ExchangeConnector<TCredentials, TState, TTradeRequest, TTradeResponse, Scratch>
where
    TCredentials: Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub fn new(
        spec: ExchangeSpec<TCredentials, TState, TTradeRequest, TTradeResponse>,
        credentials: TCredentials,
    ) -> ExchangeConnector<TCredentials, TState, TTradeRequest, TTradeResponse, Unauthenticated>
    {
        ExchangeConnector::<TCredentials, TState, TTradeRequest, TTradeResponse, Unauthenticated> {
            spec,
            credentials,
            state: TState::default(),
            _phantom_auth_state: PhantomData,
        }
    }
}

impl<TCredentials, TState, TTradeRequest, TTradeResponse>
    ExchangeConnector<TCredentials, TState, TTradeRequest, TTradeResponse, Unauthenticated>
where
    TCredentials: Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub async fn authenticate(
        self,
    ) -> StockTrekResult<
        ExchangeConnector<TCredentials, TState, TTradeRequest, TTradeResponse, Authenticated>,
    > {
        let state = self.spec.authenticate(&self.credentials).await?;
        Ok(
            ExchangeConnector::<TCredentials, TState, TTradeRequest, TTradeResponse, Authenticated> {
                spec: self.spec,
                credentials: self.credentials,
                state,
                _phantom_auth_state: PhantomData,
            },
        )
    }
}

impl<TCredentials, TState, TTradeRequest, TTradeResponse>
    ExchangeConnector<TCredentials, TState, TTradeRequest, TTradeResponse, Authenticated>
where
    TCredentials: Send + Sync,
    TState: Default + Send + Sync,
{
    pub async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TTradeRequest,
    ) -> StockTrekResult<TTradeResponse> {
        self.spec
            .send_trade_request(&self.credentials, &self.state, preferences, trade_request)
            .await
    }
}
