use crate::{
    authentication_state::{Authenticated, AuthenticationState, Scratch, Unauthenticated},
    cex::increment_sizes::IncrementSizes,
    exchange_spec::ExchangeSpec,
    sign::signer::Signer,
};
use std::{collections::HashMap, marker::PhantomData};
use stock_trek::{
    cex::trading_pair::TradingPair, error::result::StockTrekResult, preferences::Preferences,
};

pub struct ExchangeConnector<
    TTradeRequest,
    TUnsignedMessage,
    TSignedMessage,
    TTradeResponse,
    TAuthState,
> where
    TAuthState: AuthenticationState,
{
    spec: ExchangeSpec<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>,
    signer: Option<Signer<TUnsignedMessage, TSignedMessage>>,
    _phantom_auth_state: PhantomData<TAuthState>,
}

impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    ExchangeConnector<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse, Scratch>
{
    pub fn new(
        spec: ExchangeSpec<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>,
    ) -> ExchangeConnector<
        TTradeRequest,
        TUnsignedMessage,
        TSignedMessage,
        TTradeResponse,
        Unauthenticated,
    > {
        ExchangeConnector::<
            TTradeRequest,
            TUnsignedMessage,
            TSignedMessage,
            TTradeResponse,
            Unauthenticated,
        > {
            spec,
            signer: None,
            _phantom_auth_state: PhantomData,
        }
    }
}

impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    ExchangeConnector<
        TTradeRequest,
        TUnsignedMessage,
        TSignedMessage,
        TTradeResponse,
        Unauthenticated,
    >
{
    pub async fn authenticate(
        self,
        initial_auth_leg_signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<
        ExchangeConnector<
            TTradeRequest,
            TUnsignedMessage,
            TSignedMessage,
            TTradeResponse,
            Authenticated,
        >,
    > {
        let signer = self.spec.authenticate(initial_auth_leg_signer).await?;
        let authenticated_connector = ExchangeConnector::<
            TTradeRequest,
            TUnsignedMessage,
            TSignedMessage,
            TTradeResponse,
            Authenticated,
        > {
            spec: self.spec,
            signer: Some(signer),
            _phantom_auth_state: PhantomData,
        };
        Ok(authenticated_connector)
    }
}

impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    ExchangeConnector<
        TTradeRequest,
        TUnsignedMessage,
        TSignedMessage,
        TTradeResponse,
        Authenticated,
    >
{
    pub async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TTradeRequest,
        increments: &HashMap<TradingPair, IncrementSizes>,
    ) -> StockTrekResult<TTradeResponse> {
        self.spec
            .send_trade_request(
                preferences,
                trade_request,
                increments,
                self.signer.as_ref().unwrap(),
            )
            .await
    }
}
