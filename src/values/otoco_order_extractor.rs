use crate::values::extractor::ExtractorTrait;
use rust_decimal::Decimal;
use stock_trek::{asset_id::AssetId, order::orders::one_triggers_oco::OneTriggersOcoOrderGeneric};

pub type UnsignedOtOcoOrderExtractor<TUnsignedOtOcoBody> =
    Box<dyn ExtractorTrait<OneTriggersOcoOrderGeneric<AssetId, Decimal>, TUnsignedOtOcoBody>>;

pub type UnsignedOtOcoOrderFieldExtractor<TValue: Clone> =
    fn(&OneTriggersOcoOrderGeneric<AssetId, Decimal>) -> TValue;

#[allow(unused)]
macro_rules! otoco_order_extractor {
    (
        $($otoco_field_name:ident : $otoco_field_type:ty,)*
        < $state:ident, $credentials:ident >
        $($signature_field_name:ident : $signature_field_type:ty,)*
    ) => {
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct UnsignedOtOcoBody
        where
        $($otoco_field_type: Sized + Clone,)*
        {
            $(
                $otoco_field_name: $otoco_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct UnsignedOtOcoExtractor
        {
            $(
                $otoco_field_name: crate::values::otoco_order_extractor::UnsignedOtOcoOrderFieldExtractor<$otoco_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl UnsignedOtOcoExtractor {
            pub fn new(
                $(
                    $otoco_field_name: crate::values::otoco_order_extractor::UnsignedOtOcoOrderFieldExtractor<$otoco_field_type>,
                )*
            ) -> crate::values::otoco_order_extractor::UnsignedOtOcoOrderExtractor<UnsignedOtOcoBody> {
                Box::new(
                    Self {
                        $($otoco_field_name,)*
                    }
                )
            }
        }
        impl crate::values::extractor::ExtractorTrait<
            ::stock_trek::order::orders::one_triggers_oco::OneTriggersOcoOrderGeneric<
                ::stock_trek::prelude::AssetId,
                ::rust_decimal::Decimal
            >,
        UnsignedOtOcoBody> for UnsignedOtOcoExtractor {
            fn extract(
                &self,
                otoco: &::stock_trek::order::orders::one_triggers_oco::OneTriggersOcoOrderGeneric<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal
                >
            ) -> UnsignedOtOcoBody {
                UnsignedOtOcoBody {
                    $(
                        $otoco_field_name: (self.$otoco_field_name)(otoco)
                    ),*
                }
            }
        }
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct SignedOtOcoBody
        where
        $($signature_field_type: Sized,)*
        {
            $(
                $otoco_field_name: $otoco_field_type,
            )*
            $(
                $signature_field_name: $signature_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct SignedOtOcoExtractor
        {
            $(
                $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedOtOcoBody, $signature_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl SignedOtOcoExtractor {
            pub fn new(
                $(
                    $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedOtOcoBody, $signature_field_type>,
                )*
            ) -> crate::values::signer::Signer<$state, $credentials, UnsignedOtOcoBody, SignedOtOcoBody> {
                Box::new(
                    Self {
                        $($signature_field_name,)*
                    }
                )
            }
        }
        impl crate::values::signer::SignerTrait<$state, $credentials, UnsignedOtOcoBody, SignedOtOcoBody> for SignedOtOcoExtractor {
            fn sign(
                &self,
                state: &$state,
                credentials: &$credentials,
                unsigned: &UnsignedOtOcoBody
            ) -> SignedOtOcoBody {
                SignedOtOcoBody {
                    $(
                        $otoco_field_name: unsigned.$otoco_field_name.clone(),
                    )*
                    $(
                        $signature_field_name: self.$signature_field_name.sign(state, credentials, unsigned),
                    )*
                }
            }
        }
    };
}

#[allow(unused)]
pub(crate) use otoco_order_extractor;
