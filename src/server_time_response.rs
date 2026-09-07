use crate::error::{EGError, EGResult};
use exchange_types::binance::{
    response::{BinanceResponse, BinanceResponsePayload},
    time::BinanceTimeResponse,
};

pub trait ServerTimeResponse {
    fn server_time(&self) -> EGResult<u64>;
}

impl ServerTimeResponse for BinanceResponse<BinanceTimeResponse> {
    fn server_time(&self) -> EGResult<u64> {
        match &self.payload {
            BinanceResponsePayload::Success(time) => Ok(time.serverTime as u64),
            BinanceResponsePayload::Failure(error) => Err(EGError::ApiError {
                code: error.code,
                message: error.msg.clone(),
            }),
        }
    }
}
