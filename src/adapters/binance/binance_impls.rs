use crate::{
    adapters::binance::binance_structs::{
        BinanceAuthMessage, BinanceAuthReply, BinanceCredentials, BinanceHttpTransport,
        BinanceOrderReply, BinanceState,
    },
    destroy::Destroy,
    transport::transport::Transport,
    values::signed_order_request_extractor::SignedOrderRequestMessage,
};
use async_trait::async_trait;
use stock_trek::error::result::StockTrekResult;

impl BinanceState {
    pub fn new() -> Self {
        Self { id: None }
    }
}

impl Default for BinanceState {
    fn default() -> Self {
        Self::new()
    }
}

impl Destroy for BinanceCredentials {
    fn destroy(&mut self) {}
}

#[async_trait]
impl Transport<BinanceAuthMessage, BinanceAuthReply> for BinanceHttpTransport {
    // TODO
    fn new(_url: String) -> Self
    where
        Self: Sized,
    {
        BinanceHttpTransport
    }
    async fn send_and_wait_for_reply(
        &self,
        message: &BinanceAuthMessage,
        // TODO
        _timeout: chrono::Duration,
    ) -> StockTrekResult<BinanceAuthReply> {
        Ok(BinanceAuthReply {
            id: Some(message.id.clone()),
        })
    }
}

#[async_trait]
impl
    Transport<
        SignedOrderRequestMessage<
            super::binance::single::SignedOrderMessage,
            super::binance::oco::SignedOrderMessage,
            super::binance::oto::SignedOrderMessage,
            super::binance::otoco::SignedOrderMessage,
        >,
        BinanceOrderReply,
    > for BinanceHttpTransport
{
    fn new(_url: String) -> Self
    where
        Self: Sized,
    {
        BinanceHttpTransport
    }
    async fn send_and_wait_for_reply(
        &self,
        // TODO
        _message: &SignedOrderRequestMessage<
            super::binance::single::SignedOrderMessage,
            super::binance::oco::SignedOrderMessage,
            super::binance::oto::SignedOrderMessage,
            super::binance::otoco::SignedOrderMessage,
        >,
        _timeout: chrono::Duration,
    ) -> StockTrekResult<BinanceOrderReply> {
        Ok(BinanceOrderReply {
            id: Some("".to_string()),
            symbol: Some("".to_string()),
        })
    }
}
