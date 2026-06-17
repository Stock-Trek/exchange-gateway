use crate::{functions::TradeRequestToMessage, messenger::Messenger, sign::signer::Signer};
use async_trait::async_trait;
use bimap::BiMap;
use stock_trek::{
    cex::asset_id::AssetId, error::result::StockTrekResult, preferences::Preferences,
};

pub type MessageLeg<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> =
    Box<dyn MessageLegTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>>;

#[async_trait]
pub trait MessageLegTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>:
    Send + Sync
{
    async fn send_trade_request(
        &self,
        preferences: &Preferences,
        tickers: &BiMap<AssetId, String>,
        trade_request: TTradeRequest,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<TTradeResponse>;
}

pub struct MessageLegImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> {
    trade_request_to_message: TradeRequestToMessage<TTradeRequest, TUnsignedMessage>,
    messenger: Messenger<TSignedMessage, TTradeResponse>,
}

impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    MessageLegImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
where
    TTradeRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TTradeResponse: Send + Sync + 'static,
{
    pub fn new(
        trade_request_to_message: TradeRequestToMessage<TTradeRequest, TUnsignedMessage>,
        messenger: Messenger<TSignedMessage, TTradeResponse>,
    ) -> MessageLeg<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> {
        Box::new(Self {
            trade_request_to_message,
            messenger,
        })
    }
}

#[async_trait]
impl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    MessageLegTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    for MessageLegImpl<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
where
    TTradeRequest: Send + Sync,
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
    TTradeResponse: Send + Sync,
{
    async fn send_trade_request(
        &self,
        preferences: &Preferences,
        tickers: &BiMap<AssetId, String>,
        trade_request: TTradeRequest,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<TTradeResponse> {
        let unsigned = (self.trade_request_to_message)(preferences, tickers, trade_request)?;
        let signed = signer.sign(unsigned)?;
        self.messenger.send(signed).await
    }
}
