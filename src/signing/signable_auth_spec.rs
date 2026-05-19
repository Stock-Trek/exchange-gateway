use crate::{
    auth_spec::AuthSpec,
    credentials::credential::Credential,
    destroy::Destroy,
    signing::signing_method::SigningMethod,
};
use stock_trek::error::result::StockTrekResult;

/// An `AuthSpec` extended with per-message signing capabilities.
///
/// This wrapper pairs an `AuthSpec` (which handles the authentication flow)
/// with a set of `SigningMethod`s that can be applied to a `TMessage` to
/// produce cryptographic signatures.
///
/// `TMessage` is the concrete message type used by this spec for signing.
/// Each signing method is responsible for extracting bytes from the message,
/// signing them using the credential key, and packing the signature back.
pub struct SignableAuthSpec<TState, TCredentials, TTransports, TMessage>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Credential + Destroy + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
{
    auth_spec: AuthSpec<TState, TCredentials, TTransports>,
    signing_methods: Vec<SigningMethod<TState, TCredentials, TMessage>>,
}

impl<TState, TCredentials, TTransports, TMessage>
    SignableAuthSpec<TState, TCredentials, TTransports, TMessage>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Credential + Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
{
    pub fn new(
        auth_spec: AuthSpec<TState, TCredentials, TTransports>,
        signing_methods: Vec<SigningMethod<TState, TCredentials, TMessage>>,
    ) -> Self {
        Self {
            auth_spec,
            signing_methods,
        }
    }

    /// Run the authentication flow (delegates to the inner `AuthSpec`).
    pub async fn auth(
        &self,
        state: &mut TState,
        credentials: &TCredentials,
        transports: &TTransports,
    ) -> StockTrekResult<()> {
        self.auth_spec
            .auth(state, credentials, transports)
            .await
    }

    /// Sign a message using all configured signing methods.
    ///
    /// Each signing method will:
    /// 1. Extract the bytes to sign from `message` (via its `message_to_bytes` closure).
    /// 2. Sign those bytes using the credential key.
    /// 3. Pack the signature back into `message` (via its `pack_signature` closure).
    pub fn sign(
        &self,
        state: &mut TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()> {
        for method in &self.signing_methods {
            method.sign(state, credentials, message)?;
        }
        Ok(())
    }
}
