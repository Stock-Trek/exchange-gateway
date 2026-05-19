use crate::{
    credentials::credential::Credential,
    destroy::Destroy,
    signing::signing_algorithm::SigningAlgorithm,
};
use stock_trek::error::result::StockTrekResult;

/// A signing method encapsulates one signature to be applied to a `TMessage`.
///
/// It knows:
/// - Which algorithm to use (via the `Signer` trait)
/// - How to extract the message bytes from `TMessage` (via `message_to_bytes`)
/// - How to pack the resulting signature back into `TMessage` (via `pack_signature`)
///
/// This is analogous to how `gather_value` works in an `AuthLeg` — each signing
/// method is responsible for exactly one setter on `TMessage`.
pub struct SigningMethod<TState, TCredentials, TMessage>
where
    TCredentials: Credential + Destroy + Send + Sync + 'static,
{
    /// The algorithm + signer implementation
    algorithm: SigningAlgorithm,
    /// How to extract the bytes to sign from the message and optionally mutate state
    message_to_bytes:
        Box<dyn Fn(&TState, &TCredentials, &TMessage) -> StockTrekResult<Vec<u8>> + Send + Sync>,
    /// How to pack the signature bytes into the message
    pack_signature:
        Box<dyn Fn(&mut TMessage, &[u8]) -> StockTrekResult<()> + Send + Sync>,
}

impl<TState, TCredentials, TMessage> SigningMethod<TState, TCredentials, TMessage>
where
    TCredentials: Credential + Destroy + Send + Sync + 'static,
{
    /// Create a new `SigningMethod`.
    ///
    /// * `algorithm` - The signing algorithm to use (HMAC-SHA256, Ed25519, etc.)
    /// * `message_to_bytes` - A function that extracts/serializes the portion of `TMessage`
    ///   that should be signed. Receives state and credentials for context (e.g., to insert
    ///   a timestamp or nonce before signing).
    /// * `pack_signature` - A function that writes the signature bytes into `TMessage`.
    pub fn new(
        algorithm: SigningAlgorithm,
        message_to_bytes: Box<
            dyn Fn(&TState, &TCredentials, &TMessage) -> StockTrekResult<Vec<u8>> + Send + Sync,
        >,
        pack_signature: Box<
            dyn Fn(&mut TMessage, &[u8]) -> StockTrekResult<()> + Send + Sync,
        >,
    ) -> Self {
        Self {
            algorithm,
            message_to_bytes,
            pack_signature,
        }
    }

    /// Execute this signing method.
    ///
    /// 1. Extracts message bytes to sign via `message_to_bytes`.
    /// 2. Signs those bytes using the configured algorithm and the credential key.
    /// 3. Packs the signature into `TMessage` via `pack_signature`.
    pub fn sign(
        &self,
        state: &TState,
        credentials: &TCredentials,
        message: &mut TMessage,
    ) -> StockTrekResult<()> {
        let bytes_to_sign = (self.message_to_bytes)(state, credentials, message)?;
        let signer =
            crate::signing::signing_algorithm::signer_from_algorithm(self.algorithm.clone());
        let signature = signer.sign(&bytes_to_sign, credentials.credential())?;
        (self.pack_signature)(message, &signature)?;
        Ok(())
    }
}
