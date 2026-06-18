use crate::connector::Authenticator;
use stock_trek::error::result::StockTrekResult;

pub trait AuthenticatorCreatorTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
{
    fn into_authenticator(self) -> StockTrekResult<Authenticator<TTradeRequest, TTradeResponse>>;
}
