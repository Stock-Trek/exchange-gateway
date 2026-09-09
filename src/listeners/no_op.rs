use crate::{error::EGResult, listeners::listener::ListenerTrait};
use async_trait::async_trait;
use exchange_types::{encode::ByteEncoder, encrypt::Encryptor, signer::Signer};
use secrecy::SecretSlice;

pub(crate) struct NoOpListener;

impl NoOpListener {
    pub(crate) fn noop_signer() -> Signer {
        Signer::new(
            String::new(),
            Encryptor::HmacSha256(SecretSlice::from(Vec::new())),
            ByteEncoder::HexLower,
        )
    }
}

#[async_trait]
impl ListenerTrait for NoOpListener {
    type TMessage = serde_json::Value;

    async fn on_message(&self, _message: serde_json::Value) -> EGResult<()> {
        Ok(())
    }
}
