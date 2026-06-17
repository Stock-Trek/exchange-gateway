use rust_decimal::Decimal;
use stock_trek::{
    cex::{
        asset_id::AssetId,
        capability::{self, CexCapability},
        cex_preferences::CexPreferences,
        order_activation::OrderActivation,
        order_pricing::OrderPricing,
        order_quantity::OrderQuantity,
        order_request::OrderRequest,
    },
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
};

pub struct SemanticChecker;

impl SemanticChecker {
    pub fn conversion_will_be_semantically_consistent(
        &self,
        order_request: &OrderRequest<AssetId, Decimal>,
        capabilities: &[CexCapability],
        _preferences: &CexPreferences,
    ) -> StockTrekResult<()> {
        match order_request {
            OrderRequest::Single(single_order) => {
                match single_order.quantity {
                    OrderQuantity::OfQuote { .. } => {
                        match single_order.pricing {
                            OrderPricing::Limit { .. } => {
                                self.check_capability(
                                    capabilities,
                                    &CexCapability::QuoteQuantity(
                                        capability::QuoteQuantityCexCapability::AllowLimitPricing,
                                    ),
                                )?;
                            }
                            _ => {}
                        }
                        match single_order.activation {
                            OrderActivation::PriceTriggered { .. } => {
                                self.check_capability(
                                    capabilities,
                                    &CexCapability::QuoteQuantity(
                                        capability::QuoteQuantityCexCapability::AllowTriggeredTiming,
                                    ),
                                )?;
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
        }
    }
    fn check_capability(
        &self,
        capabilities: &[CexCapability],
        required_capability: &CexCapability,
    ) -> StockTrekResult<()> {
        if !capabilities.contains(required_capability) {
            return Err(StockTrekError::General(GeneralError::Message(
                "".to_string(),
            )));
        }
        Ok(())
    }
    // TODO
    // fn check_orders(
    //     &self,
    //     primary: &SingleOrderGeneric<AssetId, Decimal>,
    //     secondary: &SingleOrderGeneric<AssetId, Decimal>,
    //     capabilities: &[CexCapability],
    //     preferences: &CexPreferences,
    // ) -> StockTrekResult<()> {
    //     Self::check_value(
    //         primary,
    //         secondary,
    //         |o| &o.base,
    //         capabilities.contains(&CexCapability::MultiLeg(
    //             MultiLegCexCapability::AllowDifferentSymbol,
    //         )),
    //         preferences.multi_leg.if_different_symbol_unsupported,
    //     )?;
    //     Self::check_value(
    //         primary,
    //         secondary,
    //         |o| &o.quote,
    //         capabilities.contains(&CexCapability::MultiLeg(
    //             MultiLegCexCapability::AllowDifferentSymbol,
    //         )),
    //         preferences.multi_leg.if_different_symbol_unsupported,
    //     )?;
    //     Self::check_value(
    //         primary,
    //         secondary,
    //         |o| &o.pricing,
    //         capabilities.contains(&CexCapability::MultiLeg(
    //             MultiLegCexCapability::AllowDifferentPricing,
    //         )),
    //         preferences.multi_leg.if_different_price_unsupported,
    //     )?;
    //     Ok(())
    // }
    // fn check_value<V>(
    //     primary: &SingleOrderGeneric<AssetId, Decimal>,
    //     secondary: &SingleOrderGeneric<AssetId, Decimal>,
    //     getter: fn(&SingleOrderGeneric<AssetId, Decimal>) -> &V,
    //     can_be_different: bool,
    //     on_different: OnDifferent,
    // ) -> StockTrekResult<()>
    // where
    //     V: Eq,
    // {
    //     let a_value = getter(primary);
    //     let b_value = getter(secondary);
    //     let is_valid = (a_value == b_value)
    //         || can_be_different
    //         || (on_different == OnDifferent::UseDataFromPrimary);
    //     if !is_valid {
    //         return Err(StockTrekError::General(GeneralError::Message(
    //             "".to_string(),
    //         )));
    //     }
    //     Ok(())
    // }
}
