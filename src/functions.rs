use crate::{cex::increment_sizes::IncrementSizes, sign::signer::Signer};
use bimap::BiMap;
use std::collections::HashMap;
use stock_trek::{
    cex::{asset_id::AssetId, trading_pair::TradingPair},
    error::result::StockTrekResult,
    preferences::Preferences,
};

pub type ToIncrements<TMessage> = fn(TMessage) -> HashMap<TradingPair, IncrementSizes>;

pub type CreateAuthMessage<TAuthMessage> = fn() -> TAuthMessage;

pub type CreateSignerFrom<TAuthentication, TUnsignedMessage, TSignedMessage> =
    Box<dyn Fn(&TAuthentication) -> Signer<TUnsignedMessage, TSignedMessage> + Send + Sync>;

pub type SignatureAppender<TUnsignedMessage, TSignedMessage> =
    Box<dyn Fn(TUnsignedMessage, Option<String>) -> TSignedMessage + Send + Sync>;

pub type MessageToDto<TMessage, TDto> = fn(&TMessage) -> StockTrekResult<TDto>;

pub type DeserializeReply<TDto, TRawReply> = fn(TDto) -> StockTrekResult<TRawReply>;

pub type FilterReply<TRawReply, TReply> = fn(TRawReply) -> StockTrekResult<TReply>;

pub type RequestToUnsignedMessage<TRequest, TUnsignedMessage> =
    fn(TRequest, &Preferences, &BiMap<AssetId, String>) -> StockTrekResult<TUnsignedMessage>;

pub type ToBytes<T> = fn(&T) -> Vec<u8>;

pub type MessageConverter<TUnsignedMessage, TSignedMessage> =
    fn(TUnsignedMessage) -> TSignedMessage;
