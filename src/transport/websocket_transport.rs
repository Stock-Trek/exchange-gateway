use crate::transport::transport::Transport;
use stock_trek::error::result::StockTrekResult;

pub trait WebsocketTransport<TMessage, TReply>: Transport<WebsocketPart, TMessage, TReply> {
    fn send(&self, message: TMessage) -> StockTrekResult<()>;
    fn on_receive(&self, message: TReply) -> StockTrekResult<()>;
    fn send_and_wait_for_reply(&self, message: TMessage) -> StockTrekResult<TReply> {
        // TODO
    }
}

pub enum WebsocketPart {
    BodyPart { json_path: String },
}
