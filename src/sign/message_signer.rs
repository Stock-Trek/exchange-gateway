use crate::{
    error::EGResult,
    sign::{into_signed::IntoSigned, signer::SignerTrait},
};
use std::marker::PhantomData;

pub(crate) struct MessageSigner<TUnsignedMessage, TSignedMessage> {
    signer: exchange_types::signer::Signer,
    _marker: PhantomData<fn(TUnsignedMessage) -> TSignedMessage>,
}

impl<TUnsignedMessage, TSignedMessage> MessageSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: IntoSigned<Signed = TSignedMessage>,
{
    pub fn new(signer: exchange_types::signer::Signer) -> Self {
        Self {
            signer,
            _marker: PhantomData,
        }
    }
}

impl<TUnsignedMessage, TSignedMessage> SignerTrait<TUnsignedMessage, TSignedMessage>
    for MessageSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: IntoSigned<Signed = TSignedMessage>,
{
    fn sign(&self, unsigned: TUnsignedMessage) -> EGResult<TSignedMessage> {
        unsigned.into_signed(&self.signer)
    }
}
