use crate::values::extractor::ExtractorTrait;
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId, order::orders::one_cancels_other::OneCancelsOtherOrderGeneric,
};

pub type UnsignedOcoOrderExtractor<TUnsignedOcoBody> =
    Box<dyn ExtractorTrait<OneCancelsOtherOrderGeneric<AssetId, Decimal>, TUnsignedOcoBody>>;

pub type UnsignedOcoOrderFieldExtractor<TValue: Clone> =
    fn(&OneCancelsOtherOrderGeneric<AssetId, Decimal>) -> TValue;

#[allow(unused)]
macro_rules! oco_order_extractor {
    (
        $($oco_field_name:ident : $oco_field_type:ty,)*
        < $state:ident, $credentials:ident >
        $($signature_field_name:ident : $signature_field_type:ty,)*
    ) => {
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct UnsignedOcoBody
        where
        $($oco_field_type: Sized + Clone,)*
        {
            $(
                $oco_field_name: $oco_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct UnsignedOcoExtractor
        {
            $(
                $oco_field_name: crate::values::oco_order_extractor::UnsignedOcoOrderFieldExtractor<$oco_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl UnsignedOcoExtractor {
            pub fn new(
                $(
                    $oco_field_name: crate::values::oco_order_extractor::UnsignedOcoOrderFieldExtractor<$oco_field_type>,
                )*
            ) -> crate::values::oco_order_extractor::UnsignedOcoOrderExtractor<UnsignedOcoBody> {
                Box::new(
                    Self {
                        $($oco_field_name,)*
                    }
                )
            }
        }
        impl crate::values::extractor::ExtractorTrait<
            ::stock_trek::order::orders::one_cancels_other::OneCancelsOtherOrderGeneric<
                ::stock_trek::prelude::AssetId,
                ::rust_decimal::Decimal
            >,
        UnsignedOcoBody> for UnsignedOcoExtractor {
            fn extract(
                &self,
                oco: &::stock_trek::order::orders::one_cancels_other::OneCancelsOtherOrderGeneric<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal
                >
            ) -> UnsignedOcoBody {
                UnsignedOcoBody {
                    $(
                        $oco_field_name: (self.$oco_field_name)(oco)
                    ),*
                }
            }
        }
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct SignedOcoBody
        where
        $($signature_field_type: Sized,)*
        {
            $(
                $oco_field_name: $oco_field_type,
            )*
            $(
                $signature_field_name: $signature_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct SignedOcoExtractor
        {
            $(
                $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedOcoBody, $signature_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl SignedOcoExtractor {
            pub fn new(
                $(
                    $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedOcoBody, $signature_field_type>,
                )*
            ) -> crate::values::signer::Signer<$state, $credentials, UnsignedOcoBody, SignedOcoBody> {
                Box::new(
                    Self {
                        $($signature_field_name,)*
                    }
                )
            }
        }
        impl crate::values::signer::SignerTrait<$state, $credentials, UnsignedOcoBody, SignedOcoBody> for SignedOcoExtractor {
            fn sign(
                &self,
                state: &$state,
                credentials: &$credentials,
                unsigned: &UnsignedOcoBody
            ) -> SignedOcoBody {
                SignedOcoBody {
                    $(
                        $oco_field_name: unsigned.$oco_field_name.clone(),
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
pub(crate) use oco_order_extractor;
