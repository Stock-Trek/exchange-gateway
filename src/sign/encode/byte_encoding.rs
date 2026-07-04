use strum::Display;

#[allow(unused)]
#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ByteEncoding {
    Base16,
    Base32,
    Base58,
    Base64,
    HexLower,
    HexUpper,
}
