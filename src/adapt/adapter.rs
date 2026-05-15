use crate::{
    adapt::increment_sizes::IncrementSizes, convert::order_request_mapper::OrderRequestMapper,
};
use std::collections::HashMap;
use stock_trek::{
    asset_id::AssetId, capability::Capability, exchange_id::ExchangeId,
    order::trading_pair::TradingPair,
};

pub struct Adapter {
    pub id: ExchangeId,
    pub capabilities: Vec<Capability>,
    pub increments: HashMap<TradingPair, IncrementSizes>,
    pub symbol_ticker_divider: Option<String>,
    pub ticker_overrides: HashMap<AssetId, String>,
    pub order_request_mapper: OrderRequestMapper,
}

impl Adapter {
    pub fn new(
        id: impl AsRef<str>,
        symbol_ticker_divider: Option<impl AsRef<str>>,
        order_request_mapper: OrderRequestMapper,
    ) -> Self {
        Self {
            id: ExchangeId(id.as_ref().into()),
            capabilities: Vec::new(),
            increments: HashMap::new(),
            symbol_ticker_divider: match symbol_ticker_divider {
                None => None,
                Some(div) => Some(div.as_ref().into()),
            },
            ticker_overrides: HashMap::new(),
            order_request_mapper,
        }
    }
    pub fn add_capability(&mut self, capability: Capability) -> &mut Self {
        self.capabilities.push(capability);
        self
    }
    pub fn add_increment_sizes(
        &mut self,
        trading_pair: TradingPair,
        increment_sizes: IncrementSizes,
    ) -> &mut Self {
        self.increments.insert(trading_pair, increment_sizes);
        self
    }
    pub fn add_ticker_override(
        &mut self,
        asset_id: AssetId,
        ticker_override: impl AsRef<str>,
    ) -> &mut Self {
        self.ticker_overrides
            .insert(asset_id, ticker_override.as_ref().into());
        self
    }
    pub fn to_symbol(&self, base: &AssetId, quote: &AssetId) -> String {
        let base_ticker = self.asset_ticker(base);
        let quote_ticker = self.asset_ticker(quote);
        match &self.symbol_ticker_divider {
            None => format!("{}{}", base_ticker, quote_ticker),
            Some(divider) => format!("{}{}{}", base_ticker, divider, quote_ticker),
        }
    }
    fn asset_ticker(&self, asset_id: &AssetId) -> String {
        self.ticker_overrides
            .get(asset_id)
            .map_or(asset_id.default_ticker().to_string(), |opt| opt.clone())
    }
}
