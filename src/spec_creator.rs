use crate::exchange_spec::ExchangeSpec;
use stock_trek::error::result::StockTrekResult;

pub type SpecCreator<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> =
    Box<dyn SpecCreatorTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>>;

pub trait SpecCreatorTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> {
    fn into_spec(
        self,
    ) -> StockTrekResult<
        ExchangeSpec<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>,
    >;
}
