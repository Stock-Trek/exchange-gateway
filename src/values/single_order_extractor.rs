use crate::values::extractor::ExtractorTrait;
use rust_decimal::Decimal;
use stock_trek::{asset_id::AssetId, order::orders::single::SingleOrderGeneric};

pub type UnsignedSingleOrderExtractor<TUnsignedSingleBody> =
    Box<dyn ExtractorTrait<SingleOrderGeneric<AssetId, Decimal>, TUnsignedSingleBody>>;

pub type UnsignedSingleOrderFieldExtractor<TValue: Clone> =
    fn(&SingleOrderGeneric<AssetId, Decimal>) -> TValue;

#[allow(unused)]
macro_rules! single_order_extractor {
    (
        $($single_field_name:ident : $single_field_type:ty,)*
        < $state:ident, $credentials:ident >
        $($signature_field_name:ident : $signature_field_type:ty,)*
    ) => {
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct UnsignedSingleBody
        where
        $($single_field_type: Sized + Clone,)*
        {
            $(
                $single_field_name: $single_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct UnsignedSingleExtractor
        {
            $(
                $single_field_name: crate::values::single_order_extractor::UnsignedSingleOrderFieldExtractor<$single_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl UnsignedSingleExtractor {
            pub fn new(
                $(
                    $single_field_name: crate::values::single_order_extractor::UnsignedSingleOrderFieldExtractor<$single_field_type>,
                )*
            ) -> crate::values::single_order_extractor::UnsignedSingleOrderExtractor<UnsignedSingleBody> {
                Box::new(
                    Self {
                        $($single_field_name,)*
                    }
                )
            }
        }
        impl crate::values::extractor::ExtractorTrait<
            ::stock_trek::order::orders::single::SingleOrderGeneric<
                ::stock_trek::prelude::AssetId,
                ::rust_decimal::Decimal
            >,
        UnsignedSingleBody> for UnsignedSingleExtractor {
            fn extract(
                &self,
                single: &::stock_trek::order::orders::single::SingleOrderGeneric<
                    ::stock_trek::prelude::AssetId,
                    ::rust_decimal::Decimal
                >
            ) -> UnsignedSingleBody {
                UnsignedSingleBody {
                    $(
                        $single_field_name: (self.$single_field_name)(single)
                    ),*
                }
            }
        }
        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct SignedSingleBody
        where
        $($signature_field_type: Sized,)*
        {
            $(
                $single_field_name: $single_field_type,
            )*
            $(
                $signature_field_name: $signature_field_type,
            )*
        }
        #[allow(non_snake_case)]
        pub struct SignedSingleExtractor
        {
            $(
                $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedSingleBody, $signature_field_type>,
            )*
        }
        #[allow(non_snake_case)]
        impl SignedSingleExtractor {
            pub fn new(
                $(
                    $signature_field_name: crate::values::signer::Signer<$state, $credentials, UnsignedSingleBody, $signature_field_type>,
                )*
            ) -> crate::values::signer::Signer<$state, $credentials, UnsignedSingleBody, SignedSingleBody> {
                Box::new(
                    Self {
                        $($signature_field_name,)*
                    }
                )
            }
        }
        impl crate::values::signer::SignerTrait<$state, $credentials, UnsignedSingleBody, SignedSingleBody> for SignedSingleExtractor {
            fn sign(
                &self,
                state: &$state,
                credentials: &$credentials,
                unsigned: &UnsignedSingleBody
            ) -> SignedSingleBody {
                SignedSingleBody {
                    $(
                        $single_field_name: unsigned.$single_field_name.clone(),
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
pub(crate) use single_order_extractor;
