pub type AuthMessageExtractor<TState, TCredentials, TTransport, TExtracted> =
    Box<dyn AuthMessageExtractorTrait<TState, TCredentials, TTransport, TExtracted>>;

pub trait AuthMessageExtractorTrait<TState, TCredentials, TTransport, TAuthMessage>:
    Send + Sync
{
    fn extract(
        &self,
        state: &TState,
        credentials: &TCredentials,
        transport: &TTransport,
    ) -> TAuthMessage;
}

#[allow(unused)]
macro_rules! auth_message_extractor {
    (
        < $state:ty, $credentials:ty, $transport:ty >,
        $($field_name:ident : $field_type:ty,)*
    ) => {
        use crate::values::auth_message_extractor::{AuthMessageExtractor, AuthMessageExtractorTrait};

        pub type AuthMessageFieldExtractor<TValue> = fn(&$state, &$credentials, &$transport) -> TValue;

        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct AuthMessage {
            pub $($field_name: $field_type,)*
        }

        pub struct AuthMessageExtractorImpl {
            $($field_name: AuthMessageFieldExtractor<$field_type>,)*
        }

        impl AuthMessageExtractorImpl {
            pub fn new(
                $($field_name: AuthMessageFieldExtractor<$field_type>,)*
            ) -> AuthMessageExtractor<$state, $credentials, $transport, AuthMessage> {
                Box::new(Self {
                    $($field_name,)*
                })
            }
        }

        impl AuthMessageExtractorTrait<$state, $credentials, $transport, AuthMessage>
        for AuthMessageExtractorImpl {
            fn extract(&self, state: &$state, credentials: &$credentials, transport: &$transport) -> AuthMessage {
                $(
                    let $field_name = (self.$field_name)(state, credentials, transport);
                )*
                AuthMessage {
                    $($field_name),*
                }
            }
        }
    };
}

pub(crate) use auth_message_extractor;
