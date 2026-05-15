use crate::transport::transport::Transport;
use async_trait::async_trait;
use stock_trek::error::result::StockTrekResult;

#[async_trait]
pub trait WebsocketTransport<TMessage, TReply>: Transport<WebsocketPart, TMessage, TReply> {
    fn send(&self, message: TMessage) -> StockTrekResult<()>;
    fn on_receive(&self, message: TReply) -> StockTrekResult<()>;
    // async fn send_and_wait_for_reply(&self, message: TMessage) -> StockTrekResult<TReply> {
    // TODO
    //     Ok(())
    // }
}

pub enum WebsocketPart {
    BodyPart { json_path: String },
}
