use crate::error::EGResult;

pub(crate) type Signer<TUnsignedMessage, TSignedMessage> =
    Box<dyn SignerTrait<TUnsignedMessage, TSignedMessage>>;

pub(crate) trait SignerTrait<TUnsignedMessage, TSignedMessage>: Send + Sync {
    fn sign(&self, unsigned: TUnsignedMessage) -> EGResult<TSignedMessage>;
}
