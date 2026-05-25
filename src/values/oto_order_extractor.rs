use crate::values::extractor::ExtractorTrait;
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId, order::orders::one_triggers_other::OneTriggersOtherOrderGeneric,
};

pub type UnsignedOtoOrderExtractor<TUnsignedOtoBody> =
    Box<dyn ExtractorTrait<OneTriggersOtherOrderGeneric<AssetId, Decimal>, TUnsignedOtoBody>>;

pub type UnsignedOtoOrderFieldExtractor<TValue: Clone> =
    fn(&OneTriggersOtherOrderGeneric<AssetId, Decimal>) -> TValue;

#[allow(unused)]
macro_rules! oto_order_extractor {
    (
        $($oto_field_name:ident : $oto_field_type:ty,)*
        < $state:ident, $credentials:ident >
        $($signature_field_name:ident : $signature_field_type:ty,)*
    ) => {
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct UnsignedOtoBody
        where
        $($oto_field_type: Sized + Clone,)*
        {
            $(
                $oto_field_name: $oto_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct UnsignedOtoExtractor
        {
            $(
                $oto_field_name: crate::values::oto_order_extractor::UnsignedOtoOrderFieldExtractor<$oto_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl UnsignedOtoExtractor {
            pub fn new(
                $(
                    $oto_field_name: crate::values::oto_order_extractor::UnsignedOtoOrderFieldExtractor<$oto_field_type>,
                )*
            ) -> crate::values::oto_order_extractor::UnsignedOtoOrderExtractor<UnsignedOtoBody> {
                Box::new(
                    Self {
                        $($oto_field_name,)*
                    }
                )
            }
        }
        impl crate::values::extractor::ExtractorTrait<
            ::stock_trek::order::orders::one_triggers_other::OneTriggersOtherOrderGeneric<
                ::stock_trek::prelude::AssetId,
                ::rust_decimal::Decimal
            >,
        UnsignedOtoBody> for UnsignedOtoExtractor {
            fn extract(
                &self,
                oto: &::stock_trek::order::orders::one_triggers_other::OneTriggersOtherOrderGeneric<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal
                >
            ) -> UnsignedOtoBody {
                UnsignedOtoBody {
                    $(
                        $oto_field_name: (self.$oto_field_name)(oto)
                    ),*
                }
            }
        }
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct SignedOtoBody
        where
        $($signature_field_type: Sized,)*
        {
            $(
                $oto_field_name: $oto_field_type,
            )*
            $(
                $signature_field_name: $signature_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct SignedOtoExtractor
        {
            $(
                $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedOtoBody, $signature_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl SignedOtoExtractor {
            pub fn new(
                $(
                    $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedOtoBody, $signature_field_type>,
                )*
            ) -> crate::values::signer::Signer<$state, $credentials, UnsignedOtoBody, SignedOtoBody> {
                Box::new(
                    Self {
                        $($signature_field_name,)*
                    }
                )
            }
        }
        impl crate::values::signer::SignerTrait<$state, $credentials, UnsignedOtoBody, SignedOtoBody> for SignedOtoExtractor {
            fn sign(
                &self,
                state: &$state,
                credentials: &$credentials,
                unsigned: &UnsignedOtoBody
            ) -> SignedOtoBody {
                SignedOtoBody {
                    $(
                        $oto_field_name: unsigned.$oto_field_name.clone(),
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
pub(crate) use oto_order_extractor;
