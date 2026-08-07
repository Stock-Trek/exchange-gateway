use crate::sign::encode::byte_encoder::ByteEncoderTrait;

#[derive(Debug, Clone)]
pub(crate) struct Base32Encoder;

impl ByteEncoderTrait for Base32Encoder {
    fn encode(&self, bytes: &[u8]) -> String {
        data_encoding::BASE32.encode(bytes)
    }
}
