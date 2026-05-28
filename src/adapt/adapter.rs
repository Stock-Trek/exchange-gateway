use crate::{adapt::increment_sizes::IncrementSizes, exchange_connector::ExchangeConnector};
use std::collections::HashMap;
use stock_trek::{
    asset_id::AssetId, capability::Capability, exchange_id::ExchangeId,
    order::trading_pair::TradingPair,
};

pub struct Adapter {
    id: ExchangeId,
    capabilities: Vec<Capability>,
    increments: HashMap<TradingPair, IncrementSizes>,
    symbol_ticker_divider: Option<String>,
    tickers: HashMap<AssetId, String>,
    exchange_connector: ExchangeConnector,
}

impl Adapter {
    pub fn new(
        id: ExchangeId,
        capabilities: Vec<Capability>,
        increments: HashMap<TradingPair, IncrementSizes>,
        symbol_ticker_divider: Option<String>,
        tickers: HashMap<AssetId, String>,
        exchange_connector: ExchangeConnector,
    ) -> Self {
        Self {
            id,
            capabilities,
            increments,
            symbol_ticker_divider,
            tickers,
            exchange_connector,
        }
    }
    pub fn exchange_id(&self) -> &ExchangeId {
        &self.id
    }
    pub fn has_capability(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }
    pub fn increments_for_base_quote(
        &self,
        base: &AssetId,
        quote: &AssetId,
    ) -> Option<&IncrementSizes> {
        self.increments_for_trading_pair(&TradingPair::new(base.clone(), quote.clone()))
    }
    pub fn increments_for_trading_pair(
        &self,
        trading_pair: &TradingPair,
    ) -> Option<&IncrementSizes> {
        self.increments.get(&trading_pair)
    }
    pub fn to_symbol(&self, base: &AssetId, quote: &AssetId) -> Option<String> {
        let Some(base_ticker) = self.tickers.get(base) else {
            return None;
        };
        let Some(quote_ticker) = self.tickers.get(quote) else {
            return None;
        };
        match &self.symbol_ticker_divider {
            None => Some(format!("{}{}", base_ticker, quote_ticker)),
            Some(divider) => Some(format!("{}{}{}", base_ticker, divider, quote_ticker)),
        }
    }
    pub fn exchange_connector(&self) -> &ExchangeConnector {
        &self.exchange_connector
    }
}
