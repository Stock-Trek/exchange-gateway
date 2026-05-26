use crate::credentials::api_key_credential::ApiKeyCredentials;

pub struct BinanceHttpAdapterCreator;

pub struct BinanceState {
    pub id: Option<String>,
}

pub struct BinanceCredentials {
    pub api_key: ApiKeyCredentials,
}

pub struct BinanceHttpTransports {
    pub http: BinanceHttpTransport,
}

pub struct BinanceHttpTransport;

#[allow(non_snake_case)]
#[derive(Debug, serde::Serialize)]
pub struct BinanceAuthMessage {
    pub id: String,
}

pub struct BinanceAuthReply {
    pub id: Option<String>,
}

#[allow(non_snake_case)]
#[derive(Debug)]
pub struct BinanceUnsignedOrderMessage {
    pub symbol: String,
}

pub struct BinanceOrderReply {
    pub id: Option<String>,
    pub symbol: Option<String>,
}
