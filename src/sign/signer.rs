use stock_trek::error::result::StockTrekResult;

pub type Signer<TUnsignedMessage, TSignedMessage> =
    Box<dyn SignerTrait<TUnsignedMessage, TSignedMessage>>;

pub trait SignerTrait<TUnsignedMessage, TSignedMessage>: Send + Sync {
    fn sign(&self, unsigned: TUnsignedMessage) -> StockTrekResult<TSignedMessage>;
}
