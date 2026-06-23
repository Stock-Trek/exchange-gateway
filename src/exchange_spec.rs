use crate::{error::EGResult, sign::signer::Signer};
use async_trait::async_trait;

pub type ExchangeSpec<TRequest, TUnsignedMessage, TSignedMessage, TResponse> =
    Box<dyn ExchangeSpecTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>>;

#[async_trait]
pub trait ExchangeSpecTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>:
    Send + Sync
{
    async fn increments(&self) -> EGResult<TResponse>;
    async fn authenticate(
        &self,
        initial_signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> EGResult<Signer<TUnsignedMessage, TSignedMessage>>;
    async fn send(
        &self,
        request: TRequest,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> EGResult<TResponse>;
}
