use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ByteEncoding {
    #[allow(unused)]
    Base16,
    #[allow(unused)]
    Base32,
    #[allow(unused)]
    Base58,
    #[allow(unused)]
    Base64,
    HexLower,
    #[allow(unused)]
    HexUpper,
}
