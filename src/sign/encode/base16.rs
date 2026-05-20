use crate::sign::encode::byte_encoder::ByteEncoderTrait;

pub struct Base16Encoder;

impl ByteEncoderTrait for Base16Encoder {
    fn encode(&self, bytes: &[u8]) -> String {
        data_encoding::HEXLOWER.encode(bytes)
    }
}
