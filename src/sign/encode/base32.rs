use crate::sign::encode::byte_encoder::ByteEncoderTrait;

pub struct Base32Encoder;

impl ByteEncoderTrait for Base32Encoder {
    fn encode(&self, bytes: &[u8]) -> String {
        data_encoding::BASE32.encode(bytes)
    }
}
