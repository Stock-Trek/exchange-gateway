use crate::{
    cex::increment_sizes::IncrementSizes, exchange_spec::ExchangeSpec, sign::signer::Signer,
};
use async_trait::async_trait;
use std::collections::HashMap;
use stock_trek::{
    cex::trading_pair::TradingPair, error::result::StockTrekResult, preferences::Preferences,
};

pub type Authenticator<TTradeRequest, TTradeResponse> =
    Box<dyn AuthenticatorTrait<TTradeRequest, TTradeResponse>>;
pub type Connector<TTradeRequest, TTradeResponse> =
    Box<dyn ConnectorTrait<TTradeRequest, TTradeResponse>>;

#[async_trait]
pub trait AuthenticatorTrait<TTradeRequest, TTradeResponse> {
    async fn authenticate(self) -> StockTrekResult<Connector<TTradeRequest, TTradeResponse>>;
}
#[async_trait]
pub trait ConnectorTrait<TTradeRequest, TTradeResponse> {
    async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TTradeRequest,
        increments: &HashMap<TradingPair, IncrementSizes>,
    ) -> StockTrekResult<TTradeResponse>;
}

pub struct ConnectorImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> {
    spec: ExchangeSpec<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>,
    signer: Signer<TUnsignedMessage, TSignedMessage>,
}

impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    ConnectorImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
where
    TTradeRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TTradeResponse: Send + Sync + 'static,
{
    pub(crate) fn new(
        spec: ExchangeSpec<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>,
        initial_signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> Authenticator<TTradeRequest, TTradeResponse> {
        Box::new(ConnectorImpl {
            spec,
            signer: initial_signer,
        })
    }
}

#[async_trait]
impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    AuthenticatorTrait<TTradeRequest, TTradeResponse>
    for ConnectorImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
where
    TTradeRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TTradeResponse: Send + Sync + 'static,
{
    async fn authenticate(self) -> StockTrekResult<Connector<TTradeRequest, TTradeResponse>> {
        let ConnectorImpl { spec, signer } = self;
        let signer = spec.authenticate(signer).await?;
        Ok(Box::new(ConnectorImpl { spec, signer }))
    }
}

#[async_trait]
impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    ConnectorTrait<TTradeRequest, TTradeResponse>
    for ConnectorImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
where
    TTradeRequest: Send + Sync,
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
    TTradeResponse: Send + Sync,
{
    async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: TTradeRequest,
        increments: &HashMap<TradingPair, IncrementSizes>,
    ) -> StockTrekResult<TTradeResponse> {
        self.spec
            .send_trade_request(preferences, trade_request, increments, &self.signer)
            .await
    }
}
