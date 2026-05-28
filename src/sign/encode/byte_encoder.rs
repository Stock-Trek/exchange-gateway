use crate::sign::encode::{
    base16::{Base16EncoderLower, Base16EncoderUpper},
    base32::Base32Encoder,
    base58::Base58Encoder,
    base64::Base64Encoder,
    byte_encoding::ByteEncoding,
};

pub type ByteEncoder = Box<dyn ByteEncoderTrait>;

pub trait ByteEncoderTrait: Send + Sync {
    fn encode(&self, bytes: &[u8]) -> String;
}

impl From<ByteEncoding> for ByteEncoder {
    fn from(value: ByteEncoding) -> Self {
        match value {
            ByteEncoding::Base16 => Box::new(Base16EncoderUpper),
            ByteEncoding::Base32 => Box::new(Base32Encoder),
            ByteEncoding::Base58 => Box::new(Base58Encoder),
            ByteEncoding::Base64 => Box::new(Base64Encoder),
            ByteEncoding::HexLower => Box::new(Base16EncoderLower),
            ByteEncoding::HexUpper => Box::new(Base16EncoderUpper),
        }
    }
}
