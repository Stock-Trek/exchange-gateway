use crate::sign::encode::byte_encoder::ByteEncoderTrait;

pub(crate) struct Base16EncoderLower;
pub(crate) struct Base16EncoderUpper;

impl ByteEncoderTrait for Base16EncoderLower {
    fn encode(&self, bytes: &[u8]) -> String {
        data_encoding::HEXLOWER.encode(bytes)
    }
}
impl ByteEncoderTrait for Base16EncoderUpper {
    fn encode(&self, bytes: &[u8]) -> String {
        data_encoding::HEXUPPER.encode(bytes)
    }
}
