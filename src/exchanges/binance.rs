use crate::{
    adapters::{
        adapter::{
            ConvertedOrder, ExchangeAdapter, ExchangeAdapterTrait, ExchangeAuthentication,
            ExchangeCapabilities, ExchangeMetadata, ExchangeOrderConverter,
        },
        capability::Capability,
        increment_sizes::{IncrementSizes, IncrementSizesBuilder},
        metadata::{OrderMetadataValue, RequestPart, RestOrderMetadata, WebsocketOrderMetadata},
        transport::OrderTransport,
    },
    asset_id::AssetId,
    error::result::StockTrekResult,
    exchange_id::ExchangeId,
    order::{order_request::OrderRequest, trading_pair::TradingPair},
};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use strum::Display;
use urlencoding::encode;

pub struct BinanceAdapter {
    increments: HashMap<TradingPair, IncrementSizes>,
    capabilities: Vec<Capability>,
    rest_order_metadata: Vec<RestOrderMetadata>,
    websocket_order_metadata: Vec<WebsocketOrderMetadata>,
}

// impl OrderConverter<SingleOrder, BinanceSingleOrderParams> for BinanceAdapter {
//     fn convert_to(&self, order: &SingleOrder) -> StockTrekResult<BinanceSingleOrderParams> {
//         let base = &order.base;
//         let quote = &order.quote;
//         let activation = &order.activation;
//         let pricing = &order.pricing;
//         let side = &order.side;
//         let quantity = &order.quantity;
//         let timestamp = Utc::now().timestamp_millis();
//         let symbol = TradingPair::new(base, quote).concatenated();
//         let side = match side {
//             OrderSide::Buy => BinanceOrderSide::BUY,
//             OrderSide::Sell => BinanceOrderSide::SELL,
//         };
//         let type_ = match pricing {
//             OrderPricing::Market => match activation {
//                 OrderActivation::Immediate => BinanceOrderType::MARKET,
//                 OrderActivation::PriceTriggered { direction, .. }
//                 | OrderActivation::Trailing { direction, .. } => match direction {
//                     OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT,
//                     OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS,
//                 },
//             },
//             OrderPricing::Limit { time_in_force, .. } => match activation {
//                 OrderActivation::Immediate => {
//                     if let OrderTimeInForce::PostOnly = time_in_force {
//                         BinanceOrderType::LIMIT_MAKER
//                     } else {
//                         BinanceOrderType::LIMIT
//                     }
//                 }
//                 OrderActivation::PriceTriggered { direction, .. }
//                 | OrderActivation::Trailing { direction, .. } => match direction {
//                     OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                     OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//                 },
//             },
//         };
//         let quantity = match quantity {
//             OrderQuantity::OfBase(base_quantity) => Some(
//                 self.increments
//                     .valid_tick(base, quote, *base_quantity, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderQuantity::OfQuote(_) => None,
//         };
//         let price = match pricing {
//             OrderPricing::Market => None,
//             OrderPricing::Limit { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//         };
//         let stopPrice = match activation {
//             OrderActivation::PriceTriggered { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             _ => None,
//         };
//         let timeInForce = match pricing {
//             OrderPricing::Limit { time_in_force, .. } => match time_in_force {
//                 OrderTimeInForce::FillOrKill => Some(BinanceTimeInForce::FOK),
//                 OrderTimeInForce::ImmediateOrCancel => Some(BinanceTimeInForce::IOC),
//                 OrderTimeInForce::GoodTillTime(_) => Some(BinanceTimeInForce::GTC),
//                 OrderTimeInForce::PostOnly => Some(BinanceTimeInForce::GTX),
//             },
//             _ => None,
//         };
//         let quoteOrderQty = match quantity {
//             OrderQuantity::OfBase(_) => None,
//             OrderQuantity::OfQuote(quote_quantity) => Some(
//                 self.increments
//                     .valid_lot(base, quote, *quote_quantity, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//         };
//         let newClientOrderId = Some(Uuid::new_v4().to_string());
//         let trailingDelta = match activation {
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(callback_rate_bps.trunc() as i64),
//             _ => None,
//         };
//         let goodTillDate = match pricing {
//             OrderPricing::Limit { time_in_force, .. } => match time_in_force {
//                 OrderTimeInForce::GoodTillTime(time_millis) => Some(*time_millis as i64),
//                 _ => None,
//             },
//             _ => None,
//         };
//         Ok(BinanceSingleOrderParams {
//             timestamp,
//             apiKey: self.api_key.clone(),
//             signature: self.signature.clone(),
//             symbol,
//             side,
//             type_,
//             quantity,
//             price,
//             stopPrice,
//             timeInForce,
//             quoteOrderQty,
//             recvWindow: None,
//             newClientOrderId,
//             trailingDelta,
//             icebergQty: None,
//             strategyId: None,
//             strategyType: None,
//             selfTradePreventionMode: Some(BinanceSelfTradePreventionMode::NONE),
//             cancelRestrictions: Some(BinanceCancelRestrictions::NONE),
//             workingType: Some(BinanceWorkingType::CONTRACT_PRICE),
//             priceProtect: Some(true),
//             newOrderRespType: Some(BinanceNewOrderResponseType::FULL),
//             goodTillDate,
//         })
//     }
// }
// impl OrderConverter<OneCancelsOtherOrder, BinanceOcoParams> for BinanceAdapter {
//     fn convert_to(&self, order: &OneCancelsOtherOrder) -> StockTrekResult<BinanceOcoParams> {
//         let base = OrderValidationOptions::same_value_oco(&order, |o| o.base, |v| v.0)?;
//         let quote = OrderValidationOptions::same_value_oco(&order, |o| o.quote, |v| v.0)?;
//         let side = OrderValidationOptions::same_value_oco(&order, |o| o.side, |v| v.to_string())?;
//         let quantity =
//             OrderValidationOptions::same_value_oco(&order, |o| o.quantity, |v| v.to_string())?;
//         let timestamp = Utc::now().timestamp_millis();
//         let symbol = TradingPair::new(base, quote).concatenated();
//         let side = match &side {
//             OrderSide::Buy => BinanceOrderSide::BUY,
//             OrderSide::Sell => BinanceOrderSide::SELL,
//         };
//         let quantity = self
//             .increments
//             .valid_tick(base, quote, order.quantity, RoundingStrategy::ToZero)
//             .unwrap();
//         // Above leg (take profit)
//         let above_type = match &order.above_activation {
//             OrderActivation::PriceTriggered { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Trailing { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Immediate => BinanceOrderType::LIMIT,
//         };
//         let above_price = match &order.above_pricing {
//             OrderPricing::Limit { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderPricing::Market => None,
//         };
//         let above_stop_price = match &order.above_activation {
//             OrderActivation::PriceTriggered { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *callback_rate_bps, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderActivation::Immediate => None,
//         };
//         let above_trailing_delta = match &order.above_activation {
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(callback_rate_bps.trunc() as i64),
//             _ => None,
//         };
//         // Below leg (stop loss)
//         let below_type = match &order.below_activation {
//             OrderActivation::PriceTriggered { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Trailing { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Immediate => BinanceOrderType::LIMIT,
//         };
//         let below_price = match &order.below_pricing {
//             OrderPricing::Limit { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderPricing::Market => None,
//         };
//         let below_stop_price = match &order.below_activation {
//             OrderActivation::PriceTriggered { price, .. } => self
//                 .increments
//                 .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                 .unwrap(),
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => self
//                 .increments
//                 .valid_tick(base, quote, *callback_rate_bps, RoundingStrategy::ToZero)
//                 .unwrap(),
//             OrderActivation::Immediate => Decimal::ZERO,
//         };
//         let below_trailing_delta = match &order.below_activation {
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(callback_rate_bps.trunc() as i64),
//             _ => None,
//         };
//         Ok(BinanceOcoParams {
//             symbol,
//             listClientOrderId: Some(Uuid::new_v4().to_string()),
//             side,
//             quantity,
//             limitClientOrderId: None,
//             limitIcebergQty: None,
//             limitStrategyId: None,
//             limitStrategyType: None,
//             aboveType: above_type,
//             abovePrice: above_price,
//             aboveStopPrice: above_stop_price,
//             aboveTrailingDelta: above_trailing_delta,
//             aboveIcebergQty: None,
//             aboveTimeInForce: Some(BinanceTimeInForce::GTC),
//             aboveClientOrderId: Some(Uuid::new_v4().to_string()),
//             belowType: below_type,
//             belowPrice: below_price,
//             belowStopPrice: below_stop_price,
//             belowTrailingDelta: below_trailing_delta,
//             belowIcebergQty: None,
//             belowTimeInForce: Some(BinanceTimeInForce::GTC),
//             belowClientOrderId: Some(Uuid::new_v4().to_string()),
//             timestamp,
//             recvWindow: None,
//             apiKey: self.api_key.clone(),
//             signature: self.signature.clone(),
//             newOrderRespType: Some(BinanceNewOrderResponseType::FULL),
//             selfTradePreventionMode: Some(BinanceSelfTradePreventionMode::NONE),
//         })
//     }
// }
// impl OrderConverter<OneTriggersOtherOrder, BinanceOtoParams> for BinanceAdapter {
//     fn convert_to(&self, order: &OneTriggersOtherOrder) -> StockTrekResult<BinanceOtoParams> {
//         let timestamp = Utc::now().timestamp_millis();
//         let symbol = TradingPair::new(base, quote).concatenated();
//         // Working order (the initial order)
//         let working_side = match &order.working_order.side {
//             OrderSide::Buy => BinanceOrderSide::BUY,
//             OrderSide::Sell => BinanceOrderSide::SELL,
//         };
//         let working_quantity = self
//             .increments
//             .valid_tick(
//                 base,
//                 quote,
//                 order.working_order.quantity,
//                 RoundingStrategy::ToZero,
//             )
//             .unwrap();
//         let working_price = match &order.working_order.pricing {
//             OrderPricing::Limit { price, .. } => self
//                 .increments
//                 .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                 .unwrap(),
//             OrderPricing::Market => Decimal::ZERO,
//         };
//         let working_type = match &order.working_order.pricing {
//             OrderPricing::Limit { .. } => BinanceOrderType::LIMIT,
//             OrderPricing::Market => BinanceOrderType::MARKET,
//         };
//         // Pending order (the triggered order)
//         let pending_side = match &order.pending_order.side {
//             OrderSide::Buy => BinanceOrderSide::BUY,
//             OrderSide::Sell => BinanceOrderSide::SELL,
//         };
//         let pending_quantity = self
//             .increments
//             .valid_tick(
//                 base,
//                 quote,
//                 order.pending_order.quantity,
//                 RoundingStrategy::ToZero,
//             )
//             .unwrap();
//         let pending_price = match &order.pending_order.pricing {
//             OrderPricing::Limit { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderPricing::Market => None,
//         };
//         let pending_stop_price = match &order.pending_order.activation {
//             OrderActivation::PriceTriggered { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             _ => None,
//         };
//         let pending_trailing_delta = match &order.pending_order.activation {
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(callback_rate_bps.trunc() as i64),
//             _ => None,
//         };
//         let pending_type = match (
//             &order.pending_order.pricing,
//             &order.pending_order.activation,
//         ) {
//             (OrderPricing::Market, OrderActivation::Immediate) => BinanceOrderType::MARKET,
//             (OrderPricing::Limit { .. }, OrderActivation::Immediate) => BinanceOrderType::LIMIT,
//             (_, OrderActivation::PriceTriggered { direction, .. }) => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             (_, OrderActivation::Trailing { direction, .. }) => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//         };
//         Ok(BinanceOtoParams {
//             symbol,
//             listClientOrderId: Some(Uuid::new_v4().to_string()),
//             newOrderRespType: Some(BinanceNewOrderResponseType::FULL),
//             selfTradePreventionMode: Some(BinanceSelfTradePreventionMode::NONE),
//             workingType: working_type,
//             workingSide: working_side,
//             workingQuantity: working_quantity,
//             workingPrice: working_price,
//             workingTimeInForce: BinanceTimeInForce::GTC,
//             workingIcebergQty: None,
//             workingClientOrderId: Some(Uuid::new_v4().to_string()),
//             pendingType: pending_type,
//             pendingSide: pending_side,
//             pendingQuantity: pending_quantity,
//             pendingPrice: pending_price,
//             pendingStopPrice: pending_stop_price,
//             pendingTrailingDelta: pending_trailing_delta,
//             pendingTimeInForce: Some(BinanceTimeInForce::GTC),
//             pendingIcebergQty: None,
//             pendingClientOrderId: Some(Uuid::new_v4().to_string()),
//             timestamp,
//             recvWindow: None,
//             apiKey: self.api_key.clone(),
//             signature: self.signature.clone(),
//         })
//     }
// }
// impl OrderConverter<OneTriggersOcoOrder, BinanceOtOcoParams> for BinanceAdapter {
//     fn convert_to(&self, order: &OneTriggersOcoOrder) -> StockTrekResult<BinanceOtOcoParams> {
//         let timestamp = Utc::now().timestamp_millis();
//         let symbol = TradingPair::new(base, quote).concatenated();
//         // Working order (the initial order)
//         let working_side = match &order.working_order.side {
//             OrderSide::Buy => BinanceOrderSide::BUY,
//             OrderSide::Sell => BinanceOrderSide::SELL,
//         };
//         let working_quantity = self
//             .increments
//             .valid_tick(
//                 base,
//                 quote,
//                 order.working_order.quantity,
//                 RoundingStrategy::ToZero,
//             )
//             .unwrap();
//         let working_price = match &order.working_order.pricing {
//             OrderPricing::Limit { price, .. } => self
//                 .increments
//                 .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                 .unwrap(),
//             OrderPricing::Market => Decimal::ZERO,
//         };
//         let working_type = match &order.working_order.pricing {
//             OrderPricing::Limit { .. } => BinanceOrderType::LIMIT,
//             OrderPricing::Market => BinanceOrderType::MARKET,
//         };
//         // Pending OCO (the triggered OCO order)
//         let pending_oco = &order.pending_oco;
//         let pending_above_type = match &pending_oco.above_activation {
//             OrderActivation::PriceTriggered { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Trailing { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Immediate => BinanceOrderType::LIMIT,
//         };
//         let pending_above_price = match &pending_oco.above_pricing {
//             OrderPricing::Limit { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderPricing::Market => None,
//         };
//         let pending_above_stop_price = match &pending_oco.above_activation {
//             OrderActivation::PriceTriggered { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *callback_rate_bps, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderActivation::Immediate => None,
//         };
//         let pending_above_trailing_delta = match &pending_oco.above_activation {
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(callback_rate_bps.trunc() as i64),
//             _ => None,
//         };
//         let pending_below_type = match &pending_oco.below_activation {
//             OrderActivation::PriceTriggered { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Trailing { direction, .. } => match direction {
//                 OrderTriggerDirection::Above => BinanceOrderType::TAKE_PROFIT_LIMIT,
//                 OrderTriggerDirection::Below => BinanceOrderType::STOP_LOSS_LIMIT,
//             },
//             OrderActivation::Immediate => BinanceOrderType::LIMIT,
//         };
//         let pending_below_price = match &pending_oco.below_pricing {
//             OrderPricing::Limit { price, .. } => Some(
//                 self.increments
//                     .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                     .unwrap(),
//             ),
//             OrderPricing::Market => None,
//         };
//         let pending_below_stop_price = match &pending_oco.below_activation {
//             OrderActivation::PriceTriggered { price, .. } => self
//                 .increments
//                 .valid_tick(base, quote, *price, RoundingStrategy::ToZero)
//                 .unwrap(),
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => self
//                 .increments
//                 .valid_tick(base, quote, *callback_rate_bps, RoundingStrategy::ToZero)
//                 .unwrap(),
//             OrderActivation::Immediate => Decimal::ZERO,
//         };
//         let pending_below_trailing_delta = match &pending_oco.below_activation {
//             OrderActivation::Trailing {
//                 callback_rate_bps, ..
//             } => Some(callback_rate_bps.trunc() as i64),
//             _ => None,
//         };
//         Ok(BinanceOtOcoParams {
//             symbol,
//             listClientOrderId: Some(Uuid::new_v4().to_string()),
//             newOrderRespType: Some(BinanceNewOrderResponseType::FULL),
//             selfTradePreventionMode: Some(BinanceSelfTradePreventionMode::NONE),
//             workingType: working_type,
//             workingSide: working_side,
//             workingQuantity: working_quantity,
//             workingPrice: working_price,
//             workingTimeInForce: BinanceTimeInForce::GTC,
//             workingIcebergQty: None,
//             workingClientOrderId: Some(Uuid::new_v4().to_string()),
//             pendingAboveType: pending_above_type,
//             pendingAbovePrice: pending_above_price,
//             pendingAboveStopPrice: pending_above_stop_price,
//             pendingAboveTrailingDelta: pending_above_trailing_delta,
//             pendingAboveIcebergQty: None,
//             pendingAboveTimeInForce: Some(BinanceTimeInForce::GTC),
//             pendingAboveClientOrderId: Some(Uuid::new_v4().to_string()),
//             pendingBelowType: pending_below_type,
//             pendingBelowPrice: pending_below_price,
//             pendingBelowStopPrice: pending_below_stop_price,
//             pendingBelowTrailingDelta: pending_below_trailing_delta,
//             pendingBelowIcebergQty: None,
//             pendingBelowTimeInForce: Some(BinanceTimeInForce::GTC),
//             pendingBelowClientOrderId: Some(Uuid::new_v4().to_string()),
//             timestamp,
//             recvWindow: None,
//             apiKey: self.api_key.clone(),
//             signature: self.signature.clone(),
//         })
//     }
// }
// impl OrderConverter<SingleOrder, BinanceWebsocketWrappedParams<BinanceSingleOrderParams>>
//     for BinanceAdapter
// {
//     fn convert_to(
//         &self,
//         order: &SingleOrder,
//     ) -> StockTrekResult<BinanceWebsocketWrappedParams<BinanceSingleOrderParams>> {
//         let params: BinanceSingleOrderParams = self.convert_to(order)?;
//         self.wrap_websocket_params(params)
//     }
// }
// impl OrderConverter<OneCancelsOtherOrder, BinanceWebsocketWrappedParams<BinanceOcoParams>>
//     for BinanceAdapter
// {
//     fn convert_to(
//         &self,
//         order: &OneCancelsOtherOrder,
//     ) -> StockTrekResult<BinanceWebsocketWrappedParams<BinanceOcoParams>> {
//         let params: BinanceOcoParams = self.convert_to(order)?;
//         self.wrap_websocket_params(params)
//     }
// }
// impl OrderConverter<OneTriggersOtherOrder, BinanceWebsocketWrappedParams<BinanceOtoParams>>
//     for BinanceAdapter
// {
//     fn convert_to(
//         &self,
//         order: &OneTriggersOtherOrder,
//     ) -> StockTrekResult<BinanceWebsocketWrappedParams<BinanceOtoParams>> {
//         let params: BinanceOtoParams = self.convert_to(order)?;
//         self.wrap_websocket_params(params)
//     }
// }
// impl OrderConverter<OneTriggersOcoOrder, BinanceWebsocketWrappedParams<BinanceOtOcoParams>>
//     for BinanceAdapter
// {
//     fn convert_to(
//         &self,
//         order: &OneTriggersOcoOrder,
//     ) -> StockTrekResult<BinanceWebsocketWrappedParams<BinanceOtOcoParams>> {
//         let params: BinanceOtOcoParams = self.convert_to(order)?;
//         self.wrap_websocket_params(params)
//     }
// }

impl BinanceAdapter {
    pub fn new() -> ExchangeAdapter {
        let one = Decimal::ONE;
        let one_tenth = one / Decimal::TEN;
        let one_hundredth = one_tenth / Decimal::TEN;
        let one_thousandth = one_hundredth / Decimal::TEN;
        let increments = IncrementSizesBuilder::new()
            .with(
                AssetId::Bitcoin,
                AssetId::Tether,
                one_thousandth,
                one_thousandth,
            )
            .build();
        let capabilities = vec![
            Capability::OneTriggersOco,
            Capability::OneTriggersOther,
            Capability::OneTriggersOco,
        ];
        let rest_order_metadata = vec![
            RestOrderMetadata::new(
                OrderMetadataValue::ApiKey,
                RequestPart::query_param("X-MBX-APIKEY"),
            ),
            RestOrderMetadata::new(
                OrderMetadataValue::Signature,
                RequestPart::query_param("signature"),
            ),
        ];
        let websocket_order_metadata = vec![WebsocketOrderMetadata::new(
            OrderMetadataValue::Signature,
            "params.signature",
        )];
        Box::new(Self {
            increments,
            capabilities,
            rest_order_metadata,
            websocket_order_metadata,
        })
    }
}

impl ExchangeAdapterTrait for BinanceAdapter {}

impl ExchangeMetadata for BinanceAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId("Binance".to_string())
    }
    fn increments(&self) -> &HashMap<TradingPair, IncrementSizes> {
        &self.increments
    }
}

impl ExchangeCapabilities for BinanceAdapter {
    fn capabilities(&self) -> &Vec<Capability> {
        &self.capabilities
    }
}

impl ExchangeAuthentication for BinanceAdapter {
    fn rest_order_metadata(&self) -> &Vec<RestOrderMetadata> {
        &self.rest_order_metadata
    }
    fn websocket_order_metadata(&self) -> &Vec<WebsocketOrderMetadata> {
        &self.websocket_order_metadata
    }
}

impl ExchangeOrderConverter for BinanceAdapter {
    fn convert(
        &self,
        order: &OrderRequest<AssetId, Decimal>,
        transport: OrderTransport,
    ) -> StockTrekResult<ConvertedOrder> {
        let base = &AssetId::Bitcoin;
        let quote = &AssetId::Tether;
        let symbol = self.to_symbol(base, quote);
        let side = BinanceOrderSide::BUY;
        let type_ = BinanceOrderType::MARKET;
        let time_in_force = BinanceTimeInForce::IOC;
        let quantity = Decimal::ONE;
        let price = Decimal::ONE;
        let receive_window = 5000;
        let timestamp = Utc::now().timestamp_millis();
        let query_string = format!(
            "symbol={}&side={}&type={}&timeInForce={}&quantity={}&price={}&recvWindwo={}&timestamp={}",
            symbol, side, type_, time_in_force, quantity, price, receive_window, timestamp
        );
        let url_encoded_query_string = encode(&query_string);
        let signature_data = url_encoded_query_string.as_bytes().to_vec();
        // TODO

        let unsigned = Value::Null;
        Ok(ConvertedOrder {
            unsigned,
            signature_data,
        })
    }
}

#[derive(Debug, Display, Serialize)]
pub enum BinanceRestParams {
    Single(BinanceSingleOrderParams),
    OCO(BinanceOcoParams),
    OTO(BinanceOtoParams),
    OTOCO(BinanceOtOcoParams),
}

#[derive(Debug, Display, Serialize)]
pub enum BinanceWebsocketParams {
    Single(BinanceWebsocketWrappedParams<BinanceSingleOrderParams>),
    OCO(BinanceWebsocketWrappedParams<BinanceOcoParams>),
    OTO(BinanceWebsocketWrappedParams<BinanceOtoParams>),
    OTOCO(BinanceWebsocketWrappedParams<BinanceOtOcoParams>),
}

#[derive(Debug, Serialize)]
pub struct BinanceWebsocketWrappedParams<P> {
    id: String,
    method: String,
    params: P,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
pub struct BinanceOcoParams {
    pub symbol: String,
    pub listClientOrderId: Option<String>,
    pub side: BinanceOrderSide,
    pub quantity: Decimal,
    pub limitClientOrderId: Option<String>,
    pub limitIcebergQty: Option<Decimal>,
    pub limitStrategyId: Option<i64>,
    pub limitStrategyType: Option<i64>,
    pub aboveType: BinanceOrderType,
    pub abovePrice: Option<Decimal>,
    pub aboveStopPrice: Option<Decimal>,
    pub aboveTrailingDelta: Option<i64>,
    pub aboveIcebergQty: Option<Decimal>,
    pub aboveTimeInForce: Option<BinanceTimeInForce>,
    pub aboveClientOrderId: Option<String>,
    pub belowType: BinanceOrderType,
    pub belowPrice: Option<Decimal>,
    pub belowStopPrice: Decimal,
    pub belowTrailingDelta: Option<i64>,
    pub belowIcebergQty: Option<Decimal>,
    pub belowTimeInForce: Option<BinanceTimeInForce>,
    pub belowClientOrderId: Option<String>,
    pub timestamp: i64,
    pub recvWindow: Option<i64>,
    pub apiKey: String,
    pub signature: String,
    pub newOrderRespType: Option<BinanceNewOrderResponseType>,
    pub selfTradePreventionMode: Option<BinanceSelfTradePreventionMode>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
pub struct BinanceOtoParams {
    pub symbol: String,
    pub listClientOrderId: Option<String>,
    pub newOrderRespType: Option<BinanceNewOrderResponseType>,
    pub selfTradePreventionMode: Option<BinanceSelfTradePreventionMode>,
    pub workingType: BinanceOrderType,
    pub workingSide: BinanceOrderSide,
    pub workingQuantity: Decimal,
    pub workingPrice: Decimal,
    pub workingTimeInForce: BinanceTimeInForce,
    pub workingIcebergQty: Option<Decimal>,
    pub workingClientOrderId: Option<String>,
    pub pendingType: BinanceOrderType,
    pub pendingSide: BinanceOrderSide,
    pub pendingQuantity: Decimal,
    pub pendingPrice: Option<Decimal>,
    pub pendingStopPrice: Option<Decimal>,
    pub pendingTrailingDelta: Option<i64>,
    pub pendingTimeInForce: Option<BinanceTimeInForce>,
    pub pendingIcebergQty: Option<Decimal>,
    pub pendingClientOrderId: Option<String>,
    pub timestamp: i64,
    pub recvWindow: Option<i64>,
    pub apiKey: String,
    pub signature: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
pub struct BinanceOtOcoParams {
    pub symbol: String,
    pub listClientOrderId: Option<String>,
    pub newOrderRespType: Option<BinanceNewOrderResponseType>,
    pub selfTradePreventionMode: Option<BinanceSelfTradePreventionMode>,
    pub workingType: BinanceOrderType,
    pub workingSide: BinanceOrderSide,
    pub workingQuantity: Decimal,
    pub workingPrice: Decimal,
    pub workingTimeInForce: BinanceTimeInForce,
    pub workingIcebergQty: Option<Decimal>,
    pub workingClientOrderId: Option<String>,
    pub pendingAboveType: BinanceOrderType,
    pub pendingAbovePrice: Option<Decimal>,
    pub pendingAboveStopPrice: Option<Decimal>,
    pub pendingAboveTrailingDelta: Option<i64>,
    pub pendingAboveIcebergQty: Option<Decimal>,
    pub pendingAboveTimeInForce: Option<BinanceTimeInForce>,
    pub pendingAboveClientOrderId: Option<String>,
    pub pendingBelowType: BinanceOrderType,
    pub pendingBelowPrice: Option<Decimal>,
    pub pendingBelowStopPrice: Decimal,
    pub pendingBelowTrailingDelta: Option<i64>,
    pub pendingBelowIcebergQty: Option<Decimal>,
    pub pendingBelowTimeInForce: Option<BinanceTimeInForce>,
    pub pendingBelowClientOrderId: Option<String>,
    pub timestamp: i64,
    pub recvWindow: Option<i64>,
    pub apiKey: String,
    pub signature: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize)]
pub struct BinanceSingleOrderParams {
    timestamp: i64,
    apiKey: String,
    signature: String,
    symbol: String,
    side: BinanceOrderSide,
    #[serde(rename = "type")]
    type_: BinanceOrderType,
    quantity: Option<Decimal>,
    price: Option<Decimal>,
    stopPrice: Option<Decimal>,
    timeInForce: Option<BinanceTimeInForce>,
    quoteOrderQty: Option<Decimal>,
    recvWindow: Option<i64>,
    newClientOrderId: Option<String>,
    trailingDelta: Option<i64>,
    icebergQty: Option<Decimal>,
    strategyId: Option<i64>,
    strategyType: Option<i64>,
    selfTradePreventionMode: Option<BinanceSelfTradePreventionMode>,
    cancelRestrictions: Option<BinanceCancelRestrictions>,
    workingType: Option<BinanceWorkingType>,
    priceProtect: Option<bool>,
    newOrderRespType: Option<BinanceNewOrderResponseType>,
    goodTillDate: Option<i64>,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Serialize)]
pub enum BinanceOrderSide {
    BUY,
    SELL,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Serialize)]
pub enum BinanceOrderType {
    LIMIT,
    MARKET,
    STOP_LOSS,
    STOP_LOSS_LIMIT,
    TAKE_PROFIT,
    TAKE_PROFIT_LIMIT,
    LIMIT_MAKER,
}

#[derive(Debug, Display, Serialize)]
pub enum BinanceTimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Serialize)]
pub enum BinanceSelfTradePreventionMode {
    EXPIRE_MAKER,
    EXPIRE_TAKER,
    EXPIRE_BOTH,
    NONE,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Serialize)]
pub enum BinanceCancelRestrictions {
    ONLY_NEW,
    ONLY_PARTIALLY_FILLED,
    NONE,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Serialize)]
pub enum BinanceWorkingType {
    MARK_PRICE,
    CONTRACT_PRICE,
}

#[derive(Debug, Display, Serialize)]
pub enum BinanceNewOrderResponseType {
    ACK,
    RESULT,
    FULL,
}
