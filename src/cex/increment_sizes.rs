use rust_decimal::{Decimal, RoundingStrategy, prelude::FromPrimitive};
use std::collections::HashMap;
use stock_trek::cex::{asset_id::AssetId, trading_pair::TradingPair};

#[derive(Clone)]
pub struct IncrementSizes {
    tick_size: Decimal,
    lot_size: Decimal,
}

impl IncrementSizes {
    pub fn new(tick_size: Decimal, lot_size: Decimal) -> Self {
        Self {
            tick_size,
            lot_size,
        }
    }
    pub fn to_valid_tick(&self, tick: f64, strategy: RoundingStrategy) -> Decimal {
        Self::to_valid_decimal(tick, self.tick_size, strategy)
    }
    pub fn to_valid_lot(&self, lot: f64, strategy: RoundingStrategy) -> Decimal {
        Self::to_valid_decimal(lot, self.lot_size, strategy)
    }
    pub fn to_valid_decimal(value: f64, step: Decimal, strategy: RoundingStrategy) -> Decimal {
        let value_as_decimal = Decimal::from_f64(value).unwrap_or(Decimal::ZERO);
        if step == Decimal::ZERO {
            return value_as_decimal;
        }
        let steps = value_as_decimal / step;
        let rounded_steps = steps.round_dp_with_strategy(0, strategy);
        rounded_steps * step
    }
}

pub struct IncrementSizesBuilder {
    map: HashMap<TradingPair, IncrementSizes>,
}

impl IncrementSizesBuilder {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn with(
        &mut self,
        base: AssetId,
        quote: AssetId,
        tick_size: Decimal,
        lot_size: Decimal,
    ) -> &mut Self {
        self.map.insert(
            TradingPair::new(base, quote),
            IncrementSizes {
                tick_size,
                lot_size,
            },
        );
        self
    }
    pub fn build(&self) -> HashMap<TradingPair, IncrementSizes> {
        self.map.clone()
    }
}

impl Default for IncrementSizesBuilder {
    fn default() -> Self {
        IncrementSizesBuilder::new()
    }
}
