use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ByteEncoding {
    Base16,
    Base32,
    Base58,
    Base64,
    Hex,
}
