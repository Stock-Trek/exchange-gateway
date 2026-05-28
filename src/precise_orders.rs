use crate::increment_sizes::IncrementSizes;
use rust_decimal::{Decimal, RoundingStrategy};
use std::collections::HashMap;
use stock_trek::{
    asset_id::AssetId,
    error::{
        result::{StockTrekError, StockTrekResult},
        value::ValueError,
    },
    order::{
        order_activation::OrderActivation,
        order_pricing::OrderPricing,
        order_quantity::OrderQuantity,
        order_request::OrderRequest,
        order_trigger_direction::OrderTriggerDirection,
        orders::{
            one_cancels_other::OneCancelsOtherOrderGeneric,
            one_triggers_oco::OneTriggersOcoOrderGeneric,
            one_triggers_other::OneTriggersOtherOrderGeneric,
            single::{SingleOrder, SingleOrderGeneric},
        },
        trading_pair::TradingPair,
    },
    preferences::Rounding,
};

pub struct PreciseOrders;

impl PreciseOrders {
    pub fn precise_order_request(
        &self,
        order_request: OrderRequest<AssetId, f64>,
        increments: &HashMap<TradingPair, IncrementSizes>,
        rounding: &Rounding,
    ) -> StockTrekResult<OrderRequest<AssetId, Decimal>> {
        match order_request {
            OrderRequest::OneCancelsOther(oco) => {
                let primary = self.precise_single_order(oco.primary, increments, rounding)?;
                let secondary = self.precise_single_order(oco.secondary, increments, rounding)?;
                let precise = OneCancelsOtherOrderGeneric { primary, secondary };
                Ok(OrderRequest::OneCancelsOther(precise))
            }
            OrderRequest::OneTriggersOther(oco) => {
                let primary = self.precise_single_order(oco.primary, increments, rounding)?;
                let secondary = self.precise_single_order(oco.secondary, increments, rounding)?;
                let precise = OneTriggersOtherOrderGeneric { primary, secondary };
                Ok(OrderRequest::OneTriggersOther(precise))
            }
            OrderRequest::OneTriggersOco(oco) => {
                let primary = self.precise_single_order(oco.primary, increments, rounding)?;
                let oco_primary =
                    self.precise_single_order(oco.oco_order.primary, increments, rounding)?;
                let oco_secondary =
                    self.precise_single_order(oco.oco_order.secondary, increments, rounding)?;
                let oco_order = OneCancelsOtherOrderGeneric {
                    primary: oco_primary,
                    secondary: oco_secondary,
                };
                let precise = OneTriggersOcoOrderGeneric { primary, oco_order };
                Ok(OrderRequest::OneTriggersOco(precise))
            }
            OrderRequest::Single(single) => {
                let precise = self.precise_single_order(single, increments, rounding)?;
                Ok(OrderRequest::Single(precise))
            }
        }
    }
    fn precise_single_order(
        &self,
        single_order: SingleOrder,
        increments: &HashMap<TradingPair, IncrementSizes>,
        rounding: &Rounding,
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
                activation_price,
                basis,
                direction,
                mode,
            } => OrderActivation::PriceTriggered {
                activation_price: trading_pair_increments.to_valid_tick(
                    activation_price,
                    self.activation_price_rounding_strategy(direction, rounding),
                ),
                basis,
                direction,
                mode,
            },
            OrderActivation::Trailing {
                activation_price,
                basis,
                callback_rate_bps,
                direction,
            } => OrderActivation::Trailing {
                activation_price: trading_pair_increments.to_valid_tick(
                    activation_price,
                    self.activation_price_rounding_strategy(direction, rounding),
                ),
                basis,
                callback_rate_bps: IncrementSizes::to_valid_decimal(
                    callback_rate_bps,
                    Decimal::ONE,
                    rounding.callback_rate_bps,
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
                price: trading_pair_increments.to_valid_tick(price, rounding.price),
                time_in_force,
            },
        };
        let quantity: OrderQuantity<Decimal> = match quantity {
            // TODO this is probably wrong, uses same lot size for base and quote
            OrderQuantity::OfBase(q) => {
                OrderQuantity::OfBase(trading_pair_increments.to_valid_lot(q, rounding.quantity))
            }
            OrderQuantity::OfQuote(q) => {
                OrderQuantity::OfQuote(trading_pair_increments.to_valid_lot(q, rounding.quantity))
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
    fn activation_price_rounding_strategy(
        &self,
        direction: OrderTriggerDirection,
        rounding: &Rounding,
    ) -> RoundingStrategy {
        match direction {
            OrderTriggerDirection::Above => rounding.activation_price_triggered_above,
            OrderTriggerDirection::Below => rounding.activation_price_triggered_below,
        }
    }
}
