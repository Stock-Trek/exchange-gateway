use crate::sign::encode::byte_encoder::ByteEncoderTrait;

pub struct Base58Encoder;

impl ByteEncoderTrait for Base58Encoder {
    fn encode(&self, bytes: &[u8]) -> String {
        bs58::encode(bytes).into_string()
    }
}
