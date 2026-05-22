use crate::adapt::increment_sizes::IncrementSizes;
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
        orders::{
            one_cancels_other::OneCancelsOtherOrderGeneric,
            one_triggers_oco::OneTriggersOcoOrderGeneric,
            one_triggers_other::OneTriggersOtherOrderGeneric,
            single::{SingleOrder, SingleOrderGeneric},
        },
        trading_pair::TradingPair,
    },
};

pub struct PreciseOrders;

impl PreciseOrders {
    pub fn precise_order_request(
        order_request: OrderRequest<AssetId, f64>,
        increments: &HashMap<TradingPair, IncrementSizes>,
        price_rounding: RoundingStrategy,
        quantity_rounding: RoundingStrategy,
        rate_rounding: RoundingStrategy,
    ) -> StockTrekResult<OrderRequest<AssetId, Decimal>> {
        match order_request {
            OrderRequest::OneCancelsOther(oco) => {
                let primary = PreciseOrders::precise_single_order(
                    oco.primary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let secondary = PreciseOrders::precise_single_order(
                    oco.secondary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let precise = OneCancelsOtherOrderGeneric { primary, secondary };
                Ok(OrderRequest::OneCancelsOther(precise))
            }
            OrderRequest::OneTriggersOther(oco) => {
                let primary = PreciseOrders::precise_single_order(
                    oco.primary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let secondary = PreciseOrders::precise_single_order(
                    oco.secondary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let precise = OneTriggersOtherOrderGeneric { primary, secondary };
                Ok(OrderRequest::OneTriggersOther(precise))
            }
            OrderRequest::OneTriggersOco(oco) => {
                let primary = PreciseOrders::precise_single_order(
                    oco.primary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let oco_primary = PreciseOrders::precise_single_order(
                    oco.oco_order.primary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let oco_secondary = PreciseOrders::precise_single_order(
                    oco.oco_order.secondary,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                let oco_order = OneCancelsOtherOrderGeneric {
                    primary: oco_primary,
                    secondary: oco_secondary,
                };
                let precise = OneTriggersOcoOrderGeneric { primary, oco_order };
                Ok(OrderRequest::OneTriggersOco(precise))
            }
            OrderRequest::Single(single) => {
                let precise = PreciseOrders::precise_single_order(
                    single,
                    increments,
                    price_rounding,
                    quantity_rounding,
                    rate_rounding,
                )?;
                Ok(OrderRequest::Single(precise))
            }
        }
    }
    pub fn precise_single_order(
        single_order: SingleOrder,
        increments: &HashMap<TradingPair, IncrementSizes>,
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
                activation_price: trading_pair_increments
                    .to_valid_tick(activation_price, price_rounding),
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
