use crate::{
    authenticate_leg::{AuthenticateLeg, AuthenticateLegImpl},
    cex::{
        cex_spec::CexSpec, increment_sizes::IncrementSizes, rate_limits_weights::RequestWeights,
    },
    credentials::api_key_credential::ApiKeyCredentials,
    exchange_spec::ExchangeSpec,
    functions::{CreateAuthMessage, CreateSigner, SignatureAppender},
    increments_leg::{IncrementsLeg, IncrementsLegImpl},
    message_leg::{MessageLeg, MessageLegImpl},
    messenger::MessengerImpl,
    sign::{
        encode::byte_encoding::ByteEncoding,
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
        message_signer::MessageSigner,
    },
    spec_creator::SpecCreatorTrait,
    transports::websocket_transport::{WebsocketMessageDto, WebsocketTransportTrait},
};
use bimap::BiMap;
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use stock_trek::{
    cex::{
        asset_id::AssetId,
        capability::{CexCapability, QuoteQuantityCexCapability},
        cex_preferences::CexPreferences,
        order_activation::OrderActivation,
        order_pricing::OrderPricing,
        order_quantity::OrderQuantity,
        order_request::OrderRequest,
        order_response::OrderResponse,
        order_side::OrderSide,
        order_tag::OrderTag,
        order_time_in_force::OrderTimeInForce,
        order_trigger_direction::OrderTriggerDirection,
        trading_pair::TradingPair,
    },
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
    preferences::Preferences,
};
use uuid::Uuid;

#[derive(Serialize)]
pub struct UnsignedMessageToBinance {
    #[serde(flatten)]
    metadata: MetadataToBinance,
    params: Option<UnsignedMessageToBinanceParams>,
}

#[derive(Clone, Serialize)]
pub struct SignedMessageToBinance {
    #[serde(flatten)]
    metadata: MetadataToBinance,
    params: Option<SignedMessageToBinanceParams>,
}

#[derive(Clone, Serialize)]
pub struct MetadataToBinance {
    id: String,
    method: MethodName,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum MethodName {
    #[serde(rename = "exchangeInfo")]
    ExchangeInfo,
    #[serde(rename = "session.logon")]
    Logon,
    #[serde(rename = "session.logout")]
    Logout,
    #[serde(rename = "time")]
    Ping,
    #[serde(rename = "order.place")]
    PlaceOrder,
    #[serde(rename = "time")]
    Time,
}

#[derive(Clone, Serialize)]
pub struct SignedMessageToBinanceParams {
    #[serde(flatten)]
    signature: Option<Signature>,
    #[serde(flatten)]
    unsigned_params: UnsignedMessageToBinanceParams,
}

#[allow(non_snake_case)]
#[derive(Clone, Serialize)]
pub struct Signature {
    apiKey: String,
    signature: String,
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum UnsignedMessageToBinanceParams {
    LogonParams(UnsignedLogonParams),
    ExchangeInfoParams(ExchangeInfoParams),
    PingParams(()),
    SingleOrderParams(UnsignedSingleOrderParams),
    TimeParams(()),
}

#[allow(non_snake_case)]
#[derive(Clone, Serialize)]
pub struct UnsignedLogonParams {
    apiKey: String,
    timestamp: i64,
}

#[allow(non_snake_case)]
#[derive(Clone, Serialize)]
pub struct ExchangeInfoParams {
    permissions: Vec<String>,
    symbolStatus: String,
}

#[allow(non_snake_case)]
#[derive(Clone, Serialize)]
pub struct UnsignedSingleOrderParams {
    icebergQty: Option<Decimal>,
    newClientOrderId: String,
    newOrderRespType: NewOrderRespType,
    pegPriceType: Option<PegPriceType>,
    pegOffsetValue: Option<i32>,
    pegOffsetType: Option<PegOffsetType>,
    price: Option<Decimal>,
    quantity: Option<Decimal>,
    quoteOrderQty: Option<Decimal>,
    recvWindow: Option<Decimal>,
    selfTradePreventionMode: SelfTradeProtection,
    side: Side,
    stopPrice: Option<Decimal>,
    strategyId: Option<i64>,
    strategyType: Option<i32>,
    symbol: String,
    timeInForce: Option<TimeInForce>,
    timestamp: i64,
    trailingDelta: Option<i32>,
    r#type: OrderType,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum NewOrderRespType {
    ACK,
    RESULT,
    FULL,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum OrderType {
    LIMIT,
    LIMIT_MAKER,
    MARKET,
    STOP_LOSS,
    STOP_LOSS_LIMIT,
    TAKE_PROFIT,
    TAKE_PROFIT_LIMIT,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum PegPriceType {
    PRIMARY_PEG,
    MARKET_PEG,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum PegOffsetType {
    PRICE_LEVEL,
}

#[derive(Clone, Copy, Serialize)]
pub enum Side {
    BUY,
    SELL,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum SelfTradeProtection {
    EXPIRE_BOTH,
    EXPIRE_MAKER,
    EXPIRE_TAKER,
    DECREMENT,
    NONE,
    TRANSFER,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum TimeInForce {
    FOK,
    GTC,
    IOC,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct MessageFromBinance {
    error: Option<MessageFromBinanceError>,
    id: String,
    status: i32,
    rateLimits: Vec<MessageFromBinanceRateLimit>,
    result: MessageFromBinanceResult,
}

#[derive(Deserialize)]
pub struct MessageFromBinanceError {
    #[allow(unused)]
    code: String,
    msg: String,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize)]
#[serde(untagged)]
pub enum MessageFromBinanceResult {
    ExchangeInfo(ExchangeInfoResult),
    OrderPlaced(OrderPlaceResult),
    SessionAuthentication(SessionAuthenticationResult),
    Pong(PongResult),
    Time(TimeResult),
}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct ExchangeInfoResult {
    timezone: String,
    serverTime: i64,
    rateLimits: Vec<MessageFromBinanceRateLimit>,
    symbols: Vec<ExchangeInfoSymbol>,
}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct ExchangeInfoSymbol {
    baseAsset: String,
    baseAssetPrecision: u8,
    baseCommissionPrecision: u8,
    isSpotTradingAllowed: bool,
    orderTypes: Vec<ExchangeInfoSymbolOrderType>,
    quoteAsset: String,
    quoteAssetPrecision: u8,
    quoteCommissionPrecision: u8,
    quoteOrderQtyMarketAllowed: bool,
    quotePrecision: u8,
    status: String,
    symbol: String,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize)]
pub enum ExchangeInfoSymbolOrderType {
    LIMIT,
    LIMIT_MAKER,
    MARKET,
    STOP_LOSS_LIMIT,
    TAKE_PROFIT_LIMIT,
}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct OrderPlaceResult {
    clientOrderId: String,
    cummulativeQuoteQty: Decimal,
    executedQty: Decimal,
    orderId: i64,
    orderListId: i32,
    origQty: Decimal,
    origQuoteOrderQty: Decimal,
    price: Decimal,
    selfTradePreventionMode: String,
    side: String,
    status: String,
    symbol: String,
    timeInForce: String,
    transactTime: i64,
    r#type: String,
    workingTime: i64,
}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct MessageFromBinanceRateLimit {
    count: i64,
    interval: RateLimitInterval,
    intervalNum: i32,
    limit: i64,
    rateLimitType: RateLimitType,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize)]
pub enum RateLimitInterval {
    DAY,
    HOUR,
    MINUTE,
    SECOND,
}

#[allow(non_camel_case_types)]
#[derive(Deserialize)]
pub enum RateLimitType {
    CONNECTIONS,
    ORDERS,
    REQUEST_WEIGHT,
}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct SessionAuthenticationResult {
    apiKey: String,
    authorizedSince: i64,
    connectedSince: i64,
    returnRateLimits: bool,
    serverTime: i64,
    userDataStream: bool,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct PongResult {}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct TimeResult {
    serverTime: i64,
}

pub struct BinanceWebsocketSpecCreator {
    pub use_session: bool,
    pub credentials: ApiKeyCredentials,
    pub transport: Arc<dyn WebsocketTransportTrait>,
}

impl
    SpecCreatorTrait<
        OrderRequest<AssetId, f64>,
        UnsignedMessageToBinance,
        SignedMessageToBinance,
        OrderResponse,
    > for BinanceWebsocketSpecCreator
{
    fn into_spec(
        self,
    ) -> StockTrekResult<
        ExchangeSpec<
            OrderRequest<AssetId, f64>,
            UnsignedMessageToBinance,
            SignedMessageToBinance,
            OrderResponse,
        >,
    > {
        let BinanceWebsocketSpecCreator {
            use_session,
            credentials,
            transport,
        } = self;
        Ok(CexSpec::new(
            capabilities(),
            request_weights(),
            tickers(),
            increments_leg(transport.clone()),
            authenticate_legs(credentials, transport.clone(), use_session)?,
            message_leg(transport.clone()),
        ))
    }
}

fn capabilities() -> Vec<CexCapability> {
    vec![
        CexCapability::QuoteQuantity(QuoteQuantityCexCapability::AllowLimitPricing),
        CexCapability::QuoteQuantity(QuoteQuantityCexCapability::AllowTriggeredTiming),
    ]
}

fn request_weights() -> RequestWeights {
    RequestWeights {
        send_order_request: 1,
    }
}

fn tickers() -> BiMap<AssetId, String> {
    let mut tickers = BiMap::new();
    // TODO finish
    tickers.insert(AssetId::usdc(), "USDC".to_string());
    tickers.insert(AssetId::bitcoin(), "BTC".to_string());
    tickers
}

fn increments_leg(transport: Arc<dyn WebsocketTransportTrait>) -> IncrementsLeg {
    let timeout = Duration::seconds(30);
    let messenger = MessengerImpl::new(
        transport,
        timeout,
        to_dto,
        deserialize_reply,
        filter_reply_exchange_info,
    );
    IncrementsLegImpl::new(increments_message(), messenger, to_increments)
}

fn authenticate_legs(
    credentials: ApiKeyCredentials,
    transport: Arc<dyn WebsocketTransportTrait>,
    use_session: bool,
) -> StockTrekResult<Vec<AuthenticateLeg<UnsignedMessageToBinance, SignedMessageToBinance>>> {
    if use_session {
        let timeout = Duration::seconds(20);
        let create_auth_message = to_create_auth_message(credentials.api_key.clone());
        let messenger = MessengerImpl::new(
            transport,
            timeout,
            to_dto,
            deserialize_reply,
            filter_reply_session_authentication,
        );
        let create_signer: CreateSigner<(), UnsignedMessageToBinance, SignedMessageToBinance> =
            to_create_signer(credentials)?;
        let authentication_leg = AuthenticateLegImpl::<
            UnsignedMessageToBinance,
            SignedMessageToBinance,
            (),
        >::new(create_auth_message, messenger, create_signer);
        Ok(vec![authentication_leg])
    } else {
        Ok(vec![])
    }
}

fn message_leg(
    transport: Arc<dyn WebsocketTransportTrait>,
) -> MessageLeg<
    OrderRequest<AssetId, Decimal>,
    UnsignedMessageToBinance,
    SignedMessageToBinance,
    OrderResponse,
> {
    let timeout = Duration::seconds(10);
    let messenger = MessengerImpl::new(
        transport,
        timeout,
        to_dto,
        deserialize_reply,
        filter_reply_order_placed,
    );
    MessageLegImpl::<
        OrderRequest<AssetId, Decimal>,
        UnsignedMessageToBinance,
        SignedMessageToBinance,
        OrderResponse,
    >::new(trade_request_to_message, messenger)
}

fn increments_message() -> SignedMessageToBinance {
    let id = id();
    SignedMessageToBinance {
        metadata: MetadataToBinance {
            id,
            method: MethodName::ExchangeInfo,
        },
        params: Some(SignedMessageToBinanceParams {
            signature: None,
            unsigned_params: UnsignedMessageToBinanceParams::ExchangeInfoParams(
                ExchangeInfoParams {
                    permissions: vec!["SPOT".to_string()],
                    symbolStatus: "TRADING".to_string(),
                },
            ),
        }),
    }
}

fn to_create_auth_message(api_key: String) -> CreateAuthMessage<UnsignedMessageToBinance> {
    Box::new(move || {
        let id = id();
        let timestamp = timestamp();
        let params = UnsignedLogonParams {
            apiKey: api_key.clone(),
            timestamp,
        };
        UnsignedMessageToBinance {
            metadata: MetadataToBinance {
                id,
                method: MethodName::Logon,
            },
            params: Some(UnsignedMessageToBinanceParams::LogonParams(params)),
        }
    })
}

fn to_bytes(message: &UnsignedMessageToBinance) -> Vec<u8> {
    serde_urlencoded::to_string(message).unwrap().into_bytes()
}

fn trade_request_to_message(
    preferences: &Preferences,
    tickers: &BiMap<AssetId, String>,
    order_request: OrderRequest<AssetId, Decimal>,
) -> StockTrekResult<UnsignedMessageToBinance> {
    let id = id();
    let method = MethodName::PlaceOrder;
    let params = to_binance_params(&preferences.cex, tickers, order_request)?;
    Ok(UnsignedMessageToBinance {
        metadata: MetadataToBinance { id, method },
        params: Some(params),
    })
}

fn to_binance_params(
    preferences: &CexPreferences,
    tickers: &BiMap<AssetId, String>,
    order_request: OrderRequest<AssetId, Decimal>,
) -> StockTrekResult<UnsignedMessageToBinanceParams> {
    match order_request {
        OrderRequest::Single(single_order_request) => {
            let base = tickers.get_by_left(&single_order_request.base);
            let quote = tickers.get_by_left(&single_order_request.quote);
            if let Some(base) = base
                && let Some(quote) = quote
            {
                let price = match single_order_request.pricing {
                    OrderPricing::Market => None,
                    OrderPricing::Limit { price, .. } => Some(price),
                };
                let quantity = match single_order_request.quantity {
                    OrderQuantity::OfBase(q) => Some(q),
                    OrderQuantity::OfQuote(..) => None,
                };
                #[allow(non_snake_case)]
                let quoteOrderQty = match single_order_request.quantity {
                    OrderQuantity::OfBase(..) => None,
                    OrderQuantity::OfQuote(q) => Some(q),
                };
                #[allow(non_snake_case)]
                let recvWindow = Some(Decimal::from(preferences.max_network_delay_millis));
                let side = match single_order_request.side {
                    OrderSide::Buy => Side::BUY,
                    OrderSide::Sell => Side::SELL,
                };
                #[allow(non_snake_case)]
                let stopPrice = match single_order_request.activation {
                    OrderActivation::PriceTriggered {
                        activation_price, ..
                    } => Some(activation_price),
                    OrderActivation::Trailing {
                        activation_price, ..
                    } => Some(activation_price),
                    OrderActivation::Immediate => None,
                };
                let symbol = format!("{}{}", base, quote);
                #[allow(non_snake_case)]
                let timeInForce = match single_order_request.pricing {
                    OrderPricing::Market => None,
                    OrderPricing::Limit { time_in_force, .. } => match time_in_force {
                        OrderTimeInForce::FillOrKill => Some(TimeInForce::FOK),
                        OrderTimeInForce::GoodTillCancelled => Some(TimeInForce::GTC),
                        OrderTimeInForce::ImmediateOrCancel => Some(TimeInForce::IOC),
                    },
                };
                let r#type = match single_order_request.activation {
                    OrderActivation::PriceTriggered { direction, .. } => match direction {
                        OrderTriggerDirection::Above => match single_order_request.pricing {
                            OrderPricing::Market => OrderType::TAKE_PROFIT,
                            OrderPricing::Limit { .. } => OrderType::TAKE_PROFIT_LIMIT,
                        },
                        OrderTriggerDirection::Below => match single_order_request.pricing {
                            OrderPricing::Market => OrderType::STOP_LOSS,
                            OrderPricing::Limit { .. } => OrderType::STOP_LOSS_LIMIT,
                        },
                    },
                    OrderActivation::Trailing { direction, .. } => match direction {
                        OrderTriggerDirection::Above => match single_order_request.pricing {
                            OrderPricing::Market => OrderType::TAKE_PROFIT,
                            OrderPricing::Limit { .. } => OrderType::TAKE_PROFIT_LIMIT,
                        },
                        OrderTriggerDirection::Below => match single_order_request.pricing {
                            OrderPricing::Market => OrderType::STOP_LOSS,
                            OrderPricing::Limit { .. } => OrderType::STOP_LOSS_LIMIT,
                        },
                    },
                    OrderActivation::Immediate => match single_order_request.pricing {
                        OrderPricing::Market => OrderType::MARKET,
                        OrderPricing::Limit { .. } => OrderType::LIMIT,
                    },
                };
                let params = UnsignedSingleOrderParams {
                    icebergQty: None,
                    newClientOrderId: single_order_request.order_tag.0,
                    newOrderRespType: NewOrderRespType::FULL,
                    pegPriceType: None,
                    pegOffsetValue: None,
                    pegOffsetType: None,
                    price,
                    quantity,
                    quoteOrderQty,
                    recvWindow,
                    selfTradePreventionMode: SelfTradeProtection::NONE,
                    side,
                    stopPrice,
                    strategyId: None,
                    strategyType: None,
                    symbol,
                    timeInForce,
                    timestamp: timestamp(),
                    trailingDelta: None,
                    r#type,
                };
                Ok(UnsignedMessageToBinanceParams::SingleOrderParams(params))
            } else {
                Err(StockTrekError::General(GeneralError::Message(
                    "Failed to find ticker for base or quote".to_string(),
                )))
            }
        }
    }
}

fn to_dto(message: &SignedMessageToBinance) -> StockTrekResult<WebsocketMessageDto> {
    let body_json = serde_json::to_string(&message)
        .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
    Ok(WebsocketMessageDto { body_json })
}

fn deserialize_reply(dto: WebsocketMessageDto) -> StockTrekResult<MessageFromBinance> {
    let message: MessageFromBinance =
        serde_json::from_str(dto.body_json.as_str()).map_err(|_e| {
            StockTrekError::General(GeneralError::Message(
                "Failed to deserialize response".to_string(),
            ))
        })?;
    Ok(message)
}

fn filter_reply_session_authentication(reply: MessageFromBinance) -> StockTrekResult<()> {
    let MessageFromBinance {
        id: _id,
        result,
        error,
        status,
        rateLimits: _rate_limits,
    } = reply;
    if status >= 300 {
        let error_message = if let Some(e) = error {
            e.msg
        } else {
            "Unknown error".to_string()
        };
        return Err(StockTrekError::General(GeneralError::Message(
            error_message,
        )));
    }
    match result {
        MessageFromBinanceResult::SessionAuthentication(_session_authentication) => Ok(()),
        _ => Err(StockTrekError::General(GeneralError::Message(
            "Wrong result".to_string(),
        ))),
    }
}

fn filter_reply_order_placed(reply: MessageFromBinance) -> StockTrekResult<OrderResponse> {
    let MessageFromBinance {
        id: _id,
        result,
        error,
        status,
        rateLimits: _rate_limits,
    } = reply;
    if status >= 300 {
        let error_message = if let Some(e) = error {
            e.msg
        } else {
            "Unknown error".to_string()
        };
        return Err(StockTrekError::General(GeneralError::Message(
            error_message,
        )));
    }
    match result {
        MessageFromBinanceResult::OrderPlaced(order_placed) => Ok(OrderResponse {
            tag: OrderTag(order_placed.clientOrderId),
        }),
        _ => Err(StockTrekError::General(GeneralError::Message(
            "Wrong result".to_string(),
        ))),
    }
}

fn filter_reply_exchange_info(reply: MessageFromBinance) -> StockTrekResult<ExchangeInfoResult> {
    let MessageFromBinance {
        id: _id,
        result,
        error,
        status,
        rateLimits: _rate_limits,
    } = reply;
    if status >= 300 {
        let error_message = if let Some(e) = error {
            e.msg
        } else {
            "Unknown error".to_string()
        };
        return Err(StockTrekError::General(GeneralError::Message(
            error_message,
        )));
    }
    match result {
        MessageFromBinanceResult::ExchangeInfo(exchange_info) => Ok(exchange_info),
        _ => Err(StockTrekError::General(GeneralError::Message(
            "Wrong result".to_string(),
        ))),
    }
}

fn to_create_signer(
    credentials: ApiKeyCredentials,
) -> StockTrekResult<CreateSigner<(), UnsignedMessageToBinance, SignedMessageToBinance>> {
    let signer: DataSigner = SigningAlgorithm::Ed25519
        .signer(&credentials.secret)
        .map_err(|_e| {
            StockTrekError::General(GeneralError::Message("Cannot create signer".to_string()))
        })?;
    let create_signer: CreateSigner<(), UnsignedMessageToBinance, SignedMessageToBinance> =
        Box::new(move |_m| {
            MessageSigner::<UnsignedMessageToBinance, SignedMessageToBinance>::new(
                to_bytes,
                signer.clone(),
                ByteEncoding::Base64,
                create_signature_appender(credentials.api_key.clone()),
            )
        });
    Ok(create_signer)
}

fn create_signature_appender(
    api_key: String,
) -> SignatureAppender<UnsignedMessageToBinance, SignedMessageToBinance> {
    Box::new(move |unsigned, signature| {
        let UnsignedMessageToBinance {
            metadata,
            params: unsigned_params,
        } = unsigned;
        let params: Option<SignedMessageToBinanceParams> = if let Some(p) = unsigned_params
            && let Some(s) = signature
        {
            Some(SignedMessageToBinanceParams {
                signature: Some(Signature {
                    apiKey: api_key.to_string(),
                    signature: s,
                }),
                unsigned_params: p,
            })
        } else {
            None
        };
        SignedMessageToBinance { metadata, params }
    })
}

fn to_increments(exchange_info_result: ExchangeInfoResult) -> HashMap<TradingPair, IncrementSizes> {
    let tickers = tickers();
    let ExchangeInfoResult { symbols, .. } = exchange_info_result;
    let mut increments = HashMap::new();
    for symbol in symbols {
        let ExchangeInfoSymbol {
            baseAsset,
            baseAssetPrecision,
            quoteAsset,
            quoteAssetPrecision,
            ..
        } = symbol;
        let base = tickers.get_by_right(&baseAsset);
        let quote = tickers.get_by_right(&quoteAsset);
        if let Some(base) = base
            && let Some(quote) = quote
        {
            let trading_pair = TradingPair::new(base.clone(), quote.clone());
            let tick_size = Decimal::from_i128_with_scale(1, baseAssetPrecision as u32);
            let lot_size = Decimal::from_i128_with_scale(1, quoteAssetPrecision as u32);
            let increment_sizes = IncrementSizes::new(tick_size, lot_size);
            increments.insert(trading_pair, increment_sizes);
        }
    }
    increments
}

fn id() -> String {
    Uuid::new_v4().to_string()
}

fn timestamp() -> i64 {
    Utc::now().timestamp()
}
