#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde_with::skip_serializing_none;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", skip_serializing_none)]
#[derive(Debug, Clone)]
pub struct BinanceSignedParams<Params> {
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub params: Params,
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub signature: Option<BinanceSignature>,
}

#[allow(non_snake_case)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BinanceSignature {
    pub apiKey: String,
    pub signature: String,
}
