use crate::{
    functions::{DeserializeReply, FilterReply, MessageToDto, TradeRequestToMessage},
    sign::signer::Signer,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use bimap::BiMap;
use chrono::Duration;
use std::sync::Arc;
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

pub struct MessageLegImpl<
    TTransport,
    TTradeRequest,
    TUnsignedMessage,
    TSignedMessage,
    TRawResponse,
    TTradeResponse,
> where
    TTransport: TransportTrait + ?Sized,
{
    transport: Arc<TTransport>,
    timeout: Duration,
    trade_request_to_message: TradeRequestToMessage<TTradeRequest, TUnsignedMessage>,
    to_dto: MessageToDto<TSignedMessage, TTransport::MessageDto>,
    deserialize_reply: DeserializeReply<TTransport::MessageDto, TRawResponse>,
    filter_reply: FilterReply<TRawResponse, TTradeResponse>,
}

impl<TTransport, TTradeRequest, TUnsignedMessage, TSignedMessage, TRawResponse, TTradeResponse>
    MessageLegImpl<
        TTransport,
        TTradeRequest,
        TUnsignedMessage,
        TSignedMessage,
        TRawResponse,
        TTradeResponse,
    >
where
    TTransport: TransportTrait + ?Sized + 'static,
    TTradeRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TRawResponse: Send + Sync + 'static,
    TTradeResponse: Send + Sync + 'static,
{
    pub fn new(
        transport: Arc<TTransport>,
        timeout: Duration,
        trade_request_to_message: TradeRequestToMessage<TTradeRequest, TUnsignedMessage>,
        to_dto: MessageToDto<TSignedMessage, TTransport::MessageDto>,
        deserialize_reply: DeserializeReply<TTransport::MessageDto, TRawResponse>,
        filter_reply: FilterReply<TRawResponse, TTradeResponse>,
    ) -> MessageLeg<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse> {
        Box::new(Self {
            transport,
            timeout,
            trade_request_to_message,
            to_dto,
            deserialize_reply,
            filter_reply,
        })
    }
}

#[async_trait]
impl<TTransport, TTradeRequest, TUnsignedMessage, TSignedMessage, TRawResponse, TTradeResponse>
    MessageLegTrait<TTradeRequest, TUnsignedMessage, TSignedMessage, TTradeResponse>
    for MessageLegImpl<
        TTransport,
        TTradeRequest,
        TUnsignedMessage,
        TSignedMessage,
        TRawResponse,
        TTradeResponse,
    >
where
    TTransport: TransportTrait + Sync + ?Sized,
    TTradeRequest: Send + Sync,
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
    TRawResponse: Send + Sync,
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
        let message = (self.to_dto)(&signed)?;
        let reply = self.transport.send(message, self.timeout).await?;
        let deserialized_reply = (self.deserialize_reply)(reply)?;
        (self.filter_reply)(deserialized_reply)
    }
}
