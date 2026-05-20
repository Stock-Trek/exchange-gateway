use crate::sign::encode::byte_encoder::ByteEncoderTrait;

pub struct Base64Encoder;

impl ByteEncoderTrait for Base64Encoder {
    fn encode(&self, bytes: &[u8]) -> String {
        data_encoding::BASE64.encode(bytes)
    }
}
