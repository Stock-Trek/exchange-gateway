use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SigningAlgorithm {
    HmacSha256,
    HmacSha512,
    EcdsaP256,
    EcdsaP384,
    Ed25519,
}
