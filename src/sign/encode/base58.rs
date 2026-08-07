use crate::sign::encode::byte_encoder::ByteEncoderTrait;

#[derive(Debug, Clone)]
pub(crate) struct Base58Encoder;

impl ByteEncoderTrait for Base58Encoder {
    fn encode(&self, bytes: &[u8]) -> String {
        bs58::encode(bytes).into_string()
    }
}
