use crate::binance::{
    error::BinanceError,
    exchange_info::{BinanceExchangeInfoParams, BinanceExchangeInfoResult},
    logon::{BinanceLogonParams, BinanceSessionAuthenticationResult},
    rate_limits::BinanceRateLimit,
    signed::BinanceSignedParams,
    spot::{BinanceSpotOrderParams, BinanceSpotOrderResult},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde_with::skip_serializing_none;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
#[derive(Debug, Clone)]
pub enum BinanceWebsocketBody {
    Request(BinanceWebsocketRequest),
    Response(BinanceWebsocketResponse),
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BinanceWebsocketMetadata {
    pub id: String,
    pub method: BinanceWebsocketMethodName,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone, Copy)]
pub enum BinanceWebsocketMethodName {
    #[cfg_attr(feature = "serde", serde(rename = "exchangeInfo"))]
    ExchangeInfo,
    #[cfg_attr(feature = "serde", serde(rename = "session.logon"))]
    Logon,
    #[cfg_attr(feature = "serde", serde(rename = "session.logout"))]
    Logout,
    #[cfg_attr(feature = "serde", serde(rename = "order.place"))]
    PlaceOrder,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", skip_serializing_none)]
#[derive(Debug, Clone)]
pub struct BinanceWebsocketRequest {
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub metadata: BinanceWebsocketMetadata,
    pub params: BinanceSignedParams<BinanceWebsocketUnsignedParams>,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", skip_serializing_none)]
#[derive(Debug, Clone)]
pub struct BinanceWebsocketUnsignedRequest {
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub metadata: BinanceWebsocketMetadata,
    pub params: BinanceWebsocketUnsignedParams,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
#[derive(Debug, Clone, Hash)]
pub enum BinanceWebsocketUnsignedParams {
    ExchangeInfo(BinanceExchangeInfoParams),
    Logon(BinanceLogonParams),
    SpotOrderRequest(Box<BinanceSpotOrderParams>),
}

#[allow(non_snake_case)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Clone)]
pub struct BinanceWebsocketResponse {
    pub error: Option<BinanceError>,
    pub id: String,
    pub rateLimits: Vec<BinanceRateLimit>,
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub result: Option<BinanceWebsocketResponseResult>,
    pub status: i32,
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(untagged))]
#[derive(Debug, Clone)]
pub enum BinanceWebsocketResponseResult {
    ExchangeInfo(BinanceExchangeInfoResult),
    SessionAuthentication(BinanceSessionAuthenticationResult),
    SpotOrder(BinanceSpotOrderResult),
}
