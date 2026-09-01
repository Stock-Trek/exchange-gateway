use crate::{error::EGResult, functions::TryConvertValue, sign::signer::SignerTrait};

#[derive(Clone)]
pub(crate) struct ConvertSigner<TUnsignedMessage, TSignedMessage> {
    converter: TryConvertValue<TUnsignedMessage, TSignedMessage>,
}

impl<TUnsignedMessage, TSignedMessage> ConvertSigner<TUnsignedMessage, TSignedMessage> {
    pub fn new(converter: TryConvertValue<TUnsignedMessage, TSignedMessage>) -> Self {
        Self { converter }
    }
}

impl<TUnsignedMessage, TSignedMessage> SignerTrait<TUnsignedMessage, TSignedMessage>
    for ConvertSigner<TUnsignedMessage, TSignedMessage>
{
    fn sign(&self, unsigned: TUnsignedMessage) -> EGResult<TSignedMessage> {
        (self.converter)(unsigned)
    }
}

impl<TUnsignedMessage, TSignedMessage> std::fmt::Debug
    for ConvertSigner<TUnsignedMessage, TSignedMessage>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConvertSigner")
            .field("converter", &self.converter)
            .finish()
    }
}
