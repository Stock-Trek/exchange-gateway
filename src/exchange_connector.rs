use crate::{
    authentication_state::{Authenticated, AuthenticationState, Scratch, Unauthenticated},
    exchange_spec::ExchangeSpecTrait,
};
use std::marker::PhantomData;
use stock_trek::{error::result::StockTrekResult, preferences::Preferences};

pub struct ExchangeConnector<TSpec, TAuthState>
where
    TSpec: ExchangeSpecTrait + ?Sized,
    TAuthState: AuthenticationState,
{
    spec: Box<TSpec>,
    transports: TSpec::Transports,
    credentials: TSpec::Credentials,
    state: TSpec::State,
    _phantom_auth_state: PhantomData<TAuthState>,
}

impl<TSpec> ExchangeConnector<TSpec, Scratch>
where
    TSpec: ExchangeSpecTrait + 'static,
    TSpec::Transports: 'static,
    TSpec::Credentials: 'static,
    TSpec::State: 'static,
{
    pub fn new(
        spec: Box<TSpec>,
        transports: TSpec::Transports,
        credentials: TSpec::Credentials,
    ) -> ExchangeConnector<TSpec, Unauthenticated> {
        ExchangeConnector::<TSpec, Unauthenticated> {
            spec,
            transports,
            credentials,
            state: TSpec::State::default(),
            _phantom_auth_state: PhantomData,
        }
    }
}

impl<TSpec> ExchangeConnector<TSpec, Unauthenticated>
where
    TSpec: ExchangeSpecTrait + 'static,
    TSpec::Transports: 'static,
    TSpec::Credentials: 'static,
    TSpec::State: 'static,
{
    pub async fn authenticate(self) -> StockTrekResult<ExchangeConnector<TSpec, Authenticated>> {
        let state = self
            .spec
            .authenticate(&self.transports, &self.credentials)
            .await?;
        Ok(ExchangeConnector::<TSpec, Authenticated> {
            spec: self.spec,
            transports: self.transports,
            credentials: self.credentials,
            state,
            _phantom_auth_state: PhantomData,
        })
    }
}

impl<TSpec> ExchangeConnector<TSpec, Authenticated>
where
    TSpec: ExchangeSpecTrait,
{
    pub async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TSpec::TradeRequest,
    ) -> StockTrekResult<TSpec::TradeResponse> {
        self.spec
            .send_trade_request(
                &self.transports,
                &self.credentials,
                &self.state,
                preferences,
                trade_request,
            )
            .await
    }
}
