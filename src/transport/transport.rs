use stock_trek::error::result::StockTrekResult;

pub trait Transport<TMessagePart, TMessage, TReply> {
    fn new(url: String) -> Self;
    fn new_message(&self) -> StockTrekResult<TMessage>;
    fn send_and_wait_for_reply(
        &self,
        message: TMessage,
    ) -> impl Future<Output = StockTrekResult<TReply>>;
}
