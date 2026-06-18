use crate::{functions::RequestToUnsignedMessage, messenger::Messenger, sign::signer::Signer};
use async_trait::async_trait;
use bimap::BiMap;
use stock_trek::{
    cex::asset_id::AssetId, error::result::StockTrekResult, preferences::Preferences,
};

pub type MessageLeg<TRequest, TUnsignedMessage, TSignedMessage, TResponse> =
    Box<dyn MessageLegTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>>;

#[async_trait]
pub trait MessageLegTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>:
    Send + Sync
{
    async fn send(
        &self,
        request: TRequest,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
        preferences: &Preferences,
        tickers: &BiMap<AssetId, String>,
    ) -> StockTrekResult<TResponse>;
}

pub struct MessageLegImpl<TRequest, TUnsignedMessage, TSignedMessage, TResponse> {
    request_to_unsigned_message: RequestToUnsignedMessage<TRequest, TUnsignedMessage>,
    messenger: Messenger<TSignedMessage, TResponse>,
}

impl<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
    MessageLegImpl<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
where
    TRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub fn new(
        request_to_unsigned_message: RequestToUnsignedMessage<TRequest, TUnsignedMessage>,
        messenger: Messenger<TSignedMessage, TResponse>,
    ) -> MessageLeg<TRequest, TUnsignedMessage, TSignedMessage, TResponse> {
        Box::new(Self {
            request_to_unsigned_message,
            messenger,
        })
    }
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
    MessageLegTrait<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
    for MessageLegImpl<TRequest, TUnsignedMessage, TSignedMessage, TResponse>
where
    TRequest: Send + Sync,
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
    TResponse: Send + Sync,
{
    async fn send(
        &self,
        request: TRequest,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
        preferences: &Preferences,
        tickers: &BiMap<AssetId, String>,
    ) -> StockTrekResult<TResponse> {
        let unsigned = (self.request_to_unsigned_message)(request, preferences, tickers)?;
        let signed = signer.sign(unsigned)?;
        self.messenger.send(signed).await
    }
}
