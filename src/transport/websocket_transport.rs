use crate::transport::transport::Transport;
use stock_trek::error::result::StockTrekResult;

pub type WebsocketTransport<TMessage, TReply> = Box<dyn WebsocketTransportTrait<TMessage, TReply>>;

pub trait WebsocketTransportTrait<TMessage, TReply>: Transport<TMessage, TReply> {
    fn send(&self, message: TMessage) -> StockTrekResult<()>;
    fn on_receive(&self, message: TReply) -> StockTrekResult<()>;
    // async fn send_and_wait_for_reply(&self, message: TMessage) -> StockTrekResult<TReply> {
    // TODO
    //     Ok(())
    // }
}
