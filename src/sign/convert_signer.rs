use crate::{
    functions::MessageConverter,
    sign::signer::{Signer, SignerTrait},
};
use stock_trek::error::result::StockTrekResult;

pub struct ConvertSigner<TUnsignedMessage, TSignedMessage> {
    converter: MessageConverter<TUnsignedMessage, TSignedMessage>,
}

impl<TUnsignedMessage, TSignedMessage> ConvertSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
{
    pub fn new(
        converter: MessageConverter<TUnsignedMessage, TSignedMessage>,
    ) -> Signer<TUnsignedMessage, TSignedMessage> {
        Box::new(Self { converter })
    }
}

impl<TUnsignedMessage, TSignedMessage> SignerTrait<TUnsignedMessage, TSignedMessage>
    for ConvertSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
{
    fn sign(&self, unsigned: TUnsignedMessage) -> StockTrekResult<TSignedMessage> {
        Ok((self.converter)(unsigned))
    }
}
