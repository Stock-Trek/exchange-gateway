use crate::{
    authenticate_leg::AuthenticateLeg, increment_sizes::IncrementSizes, message_leg::MessageLeg,
};
use std::collections::HashMap;
use stock_trek::{
    asset_id::AssetId, capability::Capability, exchange_id::ExchangeId,
    order::trading_pair::TradingPair,
};

pub struct ExchangeSpec<TTransports, TCredentials, TState>
where
    TState: Default,
{
    pub id: ExchangeId,
    pub capabilities: Vec<Capability>,
    pub increments: HashMap<TradingPair, IncrementSizes>,
    pub symbol_ticker_divider: Option<String>,
    pub tickers: HashMap<AssetId, String>,
    pub authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
    pub message_leg: MessageLeg<TTransports, TCredentials, TState>,
}

impl<TTransports, TCredentials, TState> ExchangeSpec<TTransports, TCredentials, TState>
where
    TState: Default,
{
    pub fn new(
        id: ExchangeId,
        capabilities: Vec<Capability>,
        increments: HashMap<TradingPair, IncrementSizes>,
        symbol_ticker_divider: Option<String>,
        tickers: HashMap<AssetId, String>,
        authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
        message_leg: MessageLeg<TTransports, TCredentials, TState>,
    ) -> Self {
        Self {
            id,
            capabilities,
            increments,
            symbol_ticker_divider,
            tickers,
            authenticate_legs,
            message_leg,
        }
    }
    pub fn to_symbol(&self, base: &AssetId, quote: &AssetId) -> Option<String> {
        let base_ticker = self.tickers.get(base)?;
        let quote_ticker = self.tickers.get(quote)?;
        match &self.symbol_ticker_divider {
            None => Some(format!("{}{}", base_ticker, quote_ticker)),
            Some(divider) => Some(format!("{}{}{}", base_ticker, divider, quote_ticker)),
        }
    }
}
