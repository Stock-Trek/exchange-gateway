use rust_decimal::Decimal;
use stock_trek::{asset_id::AssetId, order::order_request::OrderRequest};

pub type OrderMessageExtractor<TExtracted> = Box<dyn OrderMessageExtractorTrait<TExtracted>>;

pub trait OrderMessageExtractorTrait<TExtracted>: Send + Sync {
    fn extract(&self, order_request: &OrderRequest<AssetId, Decimal>) -> TExtracted;
}

pub type OrderMessageFieldExtractor<TValue> = fn(&OrderRequest<AssetId, Decimal>) -> TValue;

#[allow(unused)]
macro_rules! order_message_extractor {
    (
      $extractor_name:ident,
      $extracted_name:ident ( $($field_name:ident : $field_type:ty),* $(,)? )
    ) => {
        use crate::values::order_message_extractor::{OrderMessageExtractor, OrderMessageExtractorTrait, OrderMessageFieldExtractor};
        use stock_trek::prelude::OrderRequest;

        pub struct $extracted_name
        where
          $($field_type: Sized,)*
        {
            $($field_name: $field_type,)*
        }

        pub struct $extractor_name
        where
          $($field_type: Sized,)*
        {
            $($field_name: OrderMessageFieldExtractor<$field_type>,)*
        }

        impl $extractor_name {
            pub fn new(
                $($field_name: OrderMessageFieldExtractor<$field_type>,)*
            ) -> OrderMessageExtractor<$extracted_name> {
                Box::new(
                    Self {
                        $($field_name,)*
                    }
                )
            }
        }

        impl OrderMessageExtractorTrait<$extracted_name> for $extractor_name {
            fn extract(&self, order_request: &OrderRequest<AssetId, Decimal>) -> $extracted_name {
                $(
                    let $field_name = (self.$field_name)(order_request);
                )*
                $extracted_name {
                    $($field_name),*
                }
            }
        }
    };
}

pub(crate) use order_message_extractor;
