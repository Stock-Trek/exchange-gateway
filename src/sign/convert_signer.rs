use crate::{error::EGResult, functions::TryConvertValue, sign::signer::SignerTrait};

pub struct ConvertSigner<TUnsignedMessage, TSignedMessage> {
    converter: TryConvertValue<TUnsignedMessage, TSignedMessage>,
}

impl<TUnsignedMessage, TSignedMessage> ConvertSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
{
    pub fn new(converter: TryConvertValue<TUnsignedMessage, TSignedMessage>) -> Self {
        Self { converter }
    }
}

impl<TUnsignedMessage, TSignedMessage> SignerTrait<TUnsignedMessage, TSignedMessage>
    for ConvertSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
{
    fn sign(&self, unsigned: TUnsignedMessage) -> EGResult<TSignedMessage> {
        (self.converter)(unsigned)
    }
}
