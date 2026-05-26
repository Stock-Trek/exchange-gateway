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
macro_rules! auth_message {
    (
        < $state:ty, $credentials:ty, $transport:ty >,
        $($auth_field_name:ident : $auth_field_type:ty,)*
    ) => {
        use crate::values::auth_message::{AuthMessageExtractor, AuthMessageExtractorTrait};

        pub type AuthMessageFieldExtractor<TValue> = fn(&$state, &$credentials, &$transport) -> TValue;

        #[allow(non_snake_case)]
        #[derive(Debug, serde::Serialize)]
        pub struct AuthMessage {
            pub $($auth_field_name: $auth_field_type,)*
        }

        pub struct AuthMessageExtractorImpl {
            $($auth_field_name: AuthMessageFieldExtractor<$auth_field_type>,)*
        }

        impl AuthMessageExtractorImpl {
            pub fn new(
                $($auth_field_name: AuthMessageFieldExtractor<$auth_field_type>,)*
            ) -> AuthMessageExtractor<$state, $credentials, $transport, AuthMessage> {
                Box::new(Self {
                    $($auth_field_name,)*
                })
            }
        }

        impl AuthMessageExtractorTrait<$state, $credentials, $transport, AuthMessage>
        for AuthMessageExtractorImpl {
            fn extract(&self, state: &$state, credentials: &$credentials, transport: &$transport) -> AuthMessage {
                $(
                    let $auth_field_name = (self.$auth_field_name)(state, credentials, transport);
                )*
                AuthMessage {
                    $($auth_field_name),*
                }
            }
        }
    };
}

pub(crate) use auth_message;
