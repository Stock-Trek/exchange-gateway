use crate::{
    functions::{SignatureAppender, ToBytes},
    sign::{
        encode::{byte_encoder::ByteEncoder, byte_encoding::ByteEncoding},
        encrypt::data_signer::DataSigner,
        signer::{Signer, SignerTrait},
    },
};
use stock_trek::error::result::StockTrekResult;

pub struct MessageSigner<TUnsignedMessage, TSignedMessage> {
    to_bytes: ToBytes<TUnsignedMessage>,
    signer: DataSigner,
    byte_encoding: ByteEncoding,
    signature_appender: SignatureAppender<TUnsignedMessage, TSignedMessage>,
}

impl<TUnsignedMessage, TSignedMessage> MessageSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
{
    pub fn new(
        to_bytes: ToBytes<TUnsignedMessage>,
        signer: DataSigner,
        byte_encoding: ByteEncoding,
        signature_appender: SignatureAppender<TUnsignedMessage, TSignedMessage>,
    ) -> Signer<TUnsignedMessage, TSignedMessage> {
        Box::new(Self {
            to_bytes,
            signer,
            byte_encoding,
            signature_appender,
        })
    }
}

impl<TUnsignedMessage, TSignedMessage> SignerTrait<TUnsignedMessage, TSignedMessage>
    for MessageSigner<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
{
    fn sign(&self, unsigned: TUnsignedMessage) -> StockTrekResult<TSignedMessage> {
        let bytes = (self.to_bytes)(&unsigned);
        let signature = if bytes.is_empty() {
            None
        } else {
            let signature_bytes = self.signer.sign(&bytes)?;
            let byte_encoder = ByteEncoder::from(self.byte_encoding);
            Some(byte_encoder.encode(&signature_bytes))
        };
        Ok((self.signature_appender)(unsigned, signature))
    }
}
