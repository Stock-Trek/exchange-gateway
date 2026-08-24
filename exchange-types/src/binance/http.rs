use crate::binance::{
    error::BinanceError,
    exchange_info::{BinanceExchangeInfoParams, BinanceExchangeInfoResult},
    filters::BinanceAssetFilter,
    signed::BinanceSignedParams,
    spot::{BinanceSpotOrderParams, BinanceSpotOrderResult},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
#[derive(Debug, Clone)]
pub enum BinanceHttpBody {
    Request(BinanceHttpRequest),
    Response(BinanceHttpResponse),
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
#[derive(Debug, Clone, Hash)]
pub enum BinanceHttpUnsignedRequest {
    AssetLimits,
    ExchangeInfo(BinanceExchangeInfoParams),
    SpotOrderRequest(Box<BinanceSpotOrderParams>),
}

pub type BinanceHttpRequest = BinanceSignedParams<BinanceHttpUnsignedRequest>;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BinanceHttpResponse {
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub result: Option<BinanceHttpResponseResult>,
    pub error: Option<BinanceError>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
#[derive(Debug, Clone)]
pub enum BinanceHttpResponseResult {
    AssetLimits(Vec<BinanceAssetFilter>),
    ExchangeInfo(BinanceExchangeInfoResult),
    SpotOrder(BinanceSpotOrderResult),
}
