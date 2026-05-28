use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    capability::{Capability, MultiLegCapability},
    order::{order_request::OrderRequest, orders::single::SingleOrderGeneric},
    preferences::{OnDifferent, Preferences},
};

pub struct SemanticChecker;

impl SemanticChecker {
    pub fn conversion_will_be_semantically_consistent(
        &self,
        order_request: &OrderRequest<AssetId, Decimal>,
        capabilities: &[Capability],
        preferences: &Preferences,
    ) -> bool {
        match order_request {
            OrderRequest::Single(_) => true,
            OrderRequest::OneCancelsOther(oco) => {
                capabilities.contains(&Capability::MultiLeg(MultiLegCapability::OneCancelsOther))
                    && self.check_orders(&oco.primary, &oco.secondary, capabilities, preferences)
            }
            OrderRequest::OneTriggersOther(oto) => {
                capabilities.contains(&Capability::MultiLeg(MultiLegCapability::OneTriggersOther))
                    && self.check_orders(&oto.primary, &oto.secondary, capabilities, preferences)
            }
            OrderRequest::OneTriggersOco(otoco) => {
                capabilities.contains(&Capability::MultiLeg(MultiLegCapability::OneTriggersOco))
                    && self.check_orders(
                        &otoco.primary,
                        &otoco.oco_order.primary,
                        capabilities,
                        preferences,
                    )
                    && self.check_orders(
                        &otoco.primary,
                        &otoco.oco_order.secondary,
                        capabilities,
                        preferences,
                    )
            }
        }
    }
    fn check_orders(
        &self,
        primary: &SingleOrderGeneric<AssetId, Decimal>,
        secondary: &SingleOrderGeneric<AssetId, Decimal>,
        capabilities: &[Capability],
        preferences: &Preferences,
    ) -> bool {
        Self::check_value(
            primary,
            secondary,
            |o| &o.base,
            capabilities.contains(&Capability::MultiLeg(
                MultiLegCapability::AllowDifferentSymbol,
            )),
            preferences.multi_leg.if_different_symbol_unsupported,
        ) && Self::check_value(
            primary,
            secondary,
            |o| &o.quote,
            capabilities.contains(&Capability::MultiLeg(
                MultiLegCapability::AllowDifferentSymbol,
            )),
            preferences.multi_leg.if_different_symbol_unsupported,
        ) && Self::check_value(
            primary,
            secondary,
            |o| &o.pricing,
            capabilities.contains(&Capability::MultiLeg(
                MultiLegCapability::AllowDifferentPricing,
            )),
            preferences.multi_leg.if_different_price_unsupported,
        )
    }
    fn check_value<V>(
        primary: &SingleOrderGeneric<AssetId, Decimal>,
        secondary: &SingleOrderGeneric<AssetId, Decimal>,
        getter: fn(&SingleOrderGeneric<AssetId, Decimal>) -> &V,
        can_be_different: bool,
        on_different: OnDifferent,
    ) -> bool
    where
        V: Eq,
    {
        let a_value = getter(primary);
        let b_value = getter(secondary);
        (a_value == b_value)
            || can_be_different
            || (on_different == OnDifferent::UseDataFromPrimary)
    }
}
