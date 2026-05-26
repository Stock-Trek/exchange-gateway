macro_rules! signed_order_variant_extractor {
    (
        $mod_name:ident,
        $order_variant:path,
        < $state:ty, $credentials:ty >,
        ( $($field_name:ident : $field_type:ty,)* ),
        $($signature_field_name:ident,)*
    ) => {
        pub mod $mod_name {
            use crate::values::extractor::ExtractorTrait;

            pub type UnsignedOrderFieldExtractor<TValue> =
                fn(&$order_variant) -> TValue;

            #[allow(non_snake_case)]
            #[derive(Debug, serde::Serialize)]
            pub struct UnsignedOrderMessage
            where
                $($field_type : Clone,)*
            {
                $(pub $field_name: $field_type,)*
            }

            #[allow(non_snake_case)]
            pub struct UnsignedOrderFieldExtractors {
                $($field_name: UnsignedOrderFieldExtractor<$field_type>,)*
            }

            #[allow(non_snake_case)]
            #[derive(Debug, serde::Serialize)]
            pub struct SignedOrderMessage {
                $(pub $field_name: $field_type,)*
                $(pub $signature_field_name: String,)*
            }

            #[allow(non_snake_case)]
            pub struct SignedOrderFieldExtractors {
                $($signature_field_name: crate::values::signer::SignatureGenerator<$state, $credentials, UnsignedOrderMessage>,)*
            }

            pub struct SignedOrderExtractor {
                unsigned: UnsignedOrderFieldExtractors,
                signed: SignedOrderFieldExtractors,
            }

            #[allow(non_snake_case)]
            impl UnsignedOrderFieldExtractors {
                pub fn new(
                    $($field_name: UnsignedOrderFieldExtractor<$field_type>,)*
                ) -> Self {
                    Self {
                        $($field_name,)*
                    }
                }
            }

            #[allow(non_snake_case)]
            impl crate::values::extractor::ExtractorTrait<$order_variant, UnsignedOrderMessage>
                for UnsignedOrderFieldExtractors
            {
                fn extract(&self, order: &$order_variant) -> UnsignedOrderMessage {
                    UnsignedOrderMessage {
                        $($field_name: (self.$field_name)(order),)*
                    }
                }
            }

            #[allow(non_snake_case)]
            impl SignedOrderFieldExtractors {
                pub fn new(
                    $($signature_field_name: crate::values::signer::SignatureGenerator<$state, $credentials, UnsignedOrderMessage>,)*
                ) -> Self {
                    Self {
                        $($signature_field_name,)*
                    }
                }
            }

            impl SignedOrderExtractor {
                pub fn new(
                    unsigned: UnsignedOrderFieldExtractors,
                    signed: SignedOrderFieldExtractors,
                ) -> Self {
                    Self {
                        unsigned,
                        signed,
                    }
                }
            }

            #[allow(non_snake_case)]
            impl
                crate::values::signer::SignerTrait<
                    $state,
                    $credentials,
                    $order_variant,
                    SignedOrderMessage,
                > for SignedOrderExtractor
            {
                fn sign(
                    &self,
                    state: &$state,
                    credentials: &$credentials,
                    order: &$order_variant,
                ) -> ::stock_trek::error::result::StockTrekResult<SignedOrderMessage> {
                    let unsigned = self.unsigned.extract(order);
                    Ok(SignedOrderMessage {
                        $($field_name: unsigned.$field_name.clone(),)*
                        $($signature_field_name: self.signed.$signature_field_name.sign(state, credentials, &unsigned)?,)*
                    })
                }
            }
        }
    };
}

pub(crate) use signed_order_variant_extractor;
