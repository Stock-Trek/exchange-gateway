use crate::increment_sizes::IncrementSizes;
use std::collections::HashMap;

pub trait ExchangeAdapter {
    fn id(&self) -> ExchangeId;
    fn capabilities(&self) -> &Vec<Capability>;
    fn increments(&self) -> &HashMap<TradingPair, IncrementSizes>;
    fn asset_ticker(&self, asset_id: &AssetId) -> &str {
        asset_id.default_ticker()
    }
    fn symbol_ticker_divider(&self) -> Option<&str> {
        None
    }
    fn to_symbol(&self, base: &AssetId, quote: &AssetId) -> String {
        let base_ticker = self.asset_ticker(base);
        let quote_ticker = self.asset_ticker(quote);
        match self.symbol_ticker_divider() {
            None => format!("{}{}", base_ticker, quote_ticker),
            Some(divider) => format!("{}{}{}", base_ticker, divider, quote_ticker),
        }
    }
    fn convert(
        &self,
        order: &OrderRequest<AssetId, Decimal>,
        transport: OrderTransport,
    ) -> StockTrekResult<ConvertedOrder>;
    fn to_precise_order(
        &self,
        single_order: SingleOrder,
        price_rounding: RoundingStrategy,
        quantity_rounding: RoundingStrategy,
        rate_rounding: RoundingStrategy,
    ) -> StockTrekResult<SingleOrderGeneric<AssetId, Decimal>> {
        let SingleOrder {
            activation,
            base,
            constraints,
            intent,
            pricing,
            quantity,
            quote,
            side,
        } = single_order;
        let increments = self.increments();
        let trading_pair_increments = increments
            .get(&TradingPair::new(base.clone(), quote.clone()))
            .ok_or_else(|| {
                StockTrekError::Value(ValueError::NotFound {
                    name: "Market".to_string(),
                    key: format!("Symbol({}/{})", base, quote),
                })
            })?;
        let activation: OrderActivation<Decimal> = match activation {
            OrderActivation::Immediate => OrderActivation::Immediate,
            OrderActivation::PriceTriggered {
                basis,
                direction,
                mode,
                price,
            } => OrderActivation::PriceTriggered {
                basis,
                direction,
                mode,
                price: trading_pair_increments.to_valid_tick(price, price_rounding),
            },
            OrderActivation::Trailing {
                activation_price,
                basis,
                callback_rate_bps,
                direction,
            } => OrderActivation::Trailing {
                activation_price: trading_pair_increments
                    .to_valid_tick(activation_price, price_rounding),
                basis,
                callback_rate_bps: IncrementSizes::to_valid_decimal(
                    callback_rate_bps,
                    Decimal::ONE,
                    rate_rounding,
                ),
                direction,
            },
        };
        let pricing: OrderPricing<Decimal> = match pricing {
            OrderPricing::Market => OrderPricing::Market,
            OrderPricing::Limit {
                price,
                time_in_force,
            } => OrderPricing::Limit {
                price: trading_pair_increments.to_valid_tick(price, price_rounding),
                time_in_force,
            },
        };
        let quantity: OrderQuantity<Decimal> = match quantity {
            // TODO this is probably wrong, uses same lot size for base and quote
            OrderQuantity::OfBase(q) => {
                OrderQuantity::OfBase(trading_pair_increments.to_valid_lot(q, quantity_rounding))
            }
            OrderQuantity::OfQuote(q) => {
                OrderQuantity::OfQuote(trading_pair_increments.to_valid_lot(q, quantity_rounding))
            }
        };
        Ok(SingleOrderGeneric::<AssetId, Decimal> {
            activation,
            base,
            constraints,
            intent,
            pricing,
            quantity,
            quote,
            side,
        })
    }
}
