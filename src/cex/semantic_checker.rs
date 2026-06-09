use rust_decimal::Decimal;
use stock_trek::{
    cex::{
        asset_id::AssetId,
        capability::{CexCapability, MultiLegCexCapability},
        cex_preferences::{CexPreferences, OnDifferent},
        order_request::OrderRequest,
        orders::single::SingleOrderGeneric,
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
        preferences: &CexPreferences,
    ) -> StockTrekResult<()> {
        match order_request {
            OrderRequest::Single(_) => Ok(()),
            OrderRequest::OneCancelsOther(oco) => {
                self.check_capability(
                    capabilities,
                    &CexCapability::MultiLeg(MultiLegCexCapability::OneCancelsOther),
                )?;
                self.check_orders(&oco.primary, &oco.secondary, capabilities, preferences)
            }
            OrderRequest::OneTriggersOther(oto) => {
                self.check_capability(
                    capabilities,
                    &CexCapability::MultiLeg(MultiLegCexCapability::OneTriggersOther),
                )?;
                self.check_orders(&oto.primary, &oto.secondary, capabilities, preferences)
            }
            OrderRequest::OneTriggersOco(otoco) => {
                self.check_capability(
                    capabilities,
                    &CexCapability::MultiLeg(MultiLegCexCapability::OneTriggersOco),
                )?;
                self.check_orders(
                    &otoco.primary,
                    &otoco.oco_order.primary,
                    capabilities,
                    preferences,
                )?;
                self.check_orders(
                    &otoco.primary,
                    &otoco.oco_order.secondary,
                    capabilities,
                    preferences,
                )
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
    fn check_orders(
        &self,
        primary: &SingleOrderGeneric<AssetId, Decimal>,
        secondary: &SingleOrderGeneric<AssetId, Decimal>,
        capabilities: &[CexCapability],
        preferences: &CexPreferences,
    ) -> StockTrekResult<()> {
        Self::check_value(
            primary,
            secondary,
            |o| &o.base,
            capabilities.contains(&CexCapability::MultiLeg(
                MultiLegCexCapability::AllowDifferentSymbol,
            )),
            preferences.multi_leg.if_different_symbol_unsupported,
        )?;
        Self::check_value(
            primary,
            secondary,
            |o| &o.quote,
            capabilities.contains(&CexCapability::MultiLeg(
                MultiLegCexCapability::AllowDifferentSymbol,
            )),
            preferences.multi_leg.if_different_symbol_unsupported,
        )?;
        Self::check_value(
            primary,
            secondary,
            |o| &o.pricing,
            capabilities.contains(&CexCapability::MultiLeg(
                MultiLegCexCapability::AllowDifferentPricing,
            )),
            preferences.multi_leg.if_different_price_unsupported,
        )?;
        Ok(())
    }
    fn check_value<V>(
        primary: &SingleOrderGeneric<AssetId, Decimal>,
        secondary: &SingleOrderGeneric<AssetId, Decimal>,
        getter: fn(&SingleOrderGeneric<AssetId, Decimal>) -> &V,
        can_be_different: bool,
        on_different: OnDifferent,
    ) -> StockTrekResult<()>
    where
        V: Eq,
    {
        let a_value = getter(primary);
        let b_value = getter(secondary);
        let is_valid = (a_value == b_value)
            || can_be_different
            || (on_different == OnDifferent::UseDataFromPrimary);
        if !is_valid {
            return Err(StockTrekError::General(GeneralError::Message(
                "".to_string(),
            )));
        }
        Ok(())
    }
}
