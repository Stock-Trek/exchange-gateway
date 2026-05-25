pub type AuthMessageExtractor<TState, TCredentials, TTransport, TExtracted> =
    Box<dyn AuthMessageExtractorTrait<TState, TCredentials, TTransport, TExtracted>>;

pub trait AuthMessageExtractorTrait<TState, TCredentials, TTransport, TExtracted>:
    Send + Sync
{
    fn extract(
        &self,
        state: &TState,
        credentials: &TCredentials,
        transport: &TTransport,
    ) -> TExtracted;
}

pub type AuthMessageFieldExtractor<TState, TCredentials, TTransport, TValue> =
    fn(&TState, &TCredentials, &TTransport) -> TValue;

#[allow(unused)]
macro_rules! auth_message_extractor {
    (
      $extractor_name:ident,
      $extracted_name:ident ( $($field_name:ident : $field_type:ty),* $(,)? )
    ) => {
        use crate::values::auth_message_extractor::{AuthMessageExtractor, AuthMessageExtractorTrait, AuthMessageFieldExtractor};

        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct $extracted_name
        where
            $($field_type: Sized,)*
        {
            $($field_name: $field_type,)*
        }

        pub struct $extractor_name<TState, TCredentials, TTransport>
        where
            TState: 'static,
            TCredentials: 'static,
            TTransport: 'static,
            $($field_type: Sized,)*
        {
            $($field_name: AuthMessageFieldExtractor<TState, TCredentials, TTransport, $field_type>,)*
        }

        impl<TState, TCredentials, TTransport> $extractor_name<TState, TCredentials, TTransport>
        where
            TState: 'static,
            TCredentials: 'static,
            TTransport: 'static,
            $($field_type: Sized,)*
        {
            pub fn new($($field_name: AuthMessageFieldExtractor<TState, TCredentials, TTransport, $field_type>,)*) -> AuthMessageExtractor<TState, TCredentials, TTransport, $extracted_name> {
                Box::new(Self {
                    $($field_name,)*
                })
            }
        }

        impl<TState, TCredentials, TTransport>
        AuthMessageExtractorTrait<TState, TCredentials, TTransport, $extracted_name>
        for $extractor_name<TState, TCredentials, TTransport> {
            fn extract(&self, state: &TState, credentials: &TCredentials, transport: &TTransport) -> $extracted_name {
                $(
                    let $field_name = (self.$field_name)(state, credentials, transport);
                )*
                $extracted_name {
                    $($field_name),*
                }
            }
        }
    };
}

pub(crate) use auth_message_extractor;
