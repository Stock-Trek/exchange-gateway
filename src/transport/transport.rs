use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

#[async_trait]
pub trait Transport<TMessage, TReply> {
    fn new(url: String) -> Self
    where
        Self: Sized;
    fn new_message(&self) -> StockTrekResult<TMessage>;
    async fn send_and_wait_for_reply(
        &self,
        message: &TMessage,
        timeout: Duration,
    ) -> StockTrekResult<TReply>;
}
