use crate::{
    adapt::{
        adapter::Adapter,
        adapter_creator::AdapterCreatorTrait,
        increment_sizes::{IncrementSizes, IncrementSizesBuilder},
    },
    auth_spec::AuthSpec,
    authenticate_leg::AuthenticateLegImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    destroy::Destroy,
    exchange_connector::ExchangeConnectorImpl,
    message_leg::MessageLeg,
    sign::{encode::byte_encoding::ByteEncoding, encrypt::signing_algorithm::SigningAlgorithm},
    transport::transport::Transport,
    values::{
        auth_message_extractor::auth_message_extractor, oco_order_extractor::oco_order_extractor,
        order_message_signer::OrderMessageSigner, order_request_extractor::order_request_extractor,
        order_response_extractor::OrderResponseExtractor, oto_order_extractor::oto_order_extractor,
        otoco_order_extractor::otoco_order_extractor,
        single_order_extractor::single_order_extractor, store_auth_value::StoreAuthValueImpl,
    },
};
use async_trait::async_trait;
use chrono::Duration;
use rust_decimal::Decimal;
use serde::Serialize;
use std::collections::HashMap;
use stock_trek::{
    asset_id::AssetId,
    capability::{Capability, MultiLegCapability, QuoteQuantityCapability},
    error::result::StockTrekResult,
    exchange_id::ExchangeId,
    order::trading_pair::TradingPair,
};
use strum::Display;

pub struct BinanceHttpAdapterCreator;
pub struct BinanceWebsocketAdapterCreator;
pub struct BinanceState {
    id: Option<String>,
}
pub struct BinanceCredentials {
    api_key: ApiKeyCredentials,
}
pub struct BinanceHttpTransports {
    http: BinanceHttpTransport,
}
pub struct BinanceHttpTransport;

impl BinanceState {
    pub fn new() -> Self {
        Self { id: None }
    }
}
impl Default for BinanceState {
    fn default() -> Self {
        Self::new()
    }
}
impl Destroy for BinanceCredentials {
    fn destroy(&mut self) {}
}

pub struct BinanceAuthReply {
    id: Option<String>,
}

pub struct BinanceOrderReply {
    id: Option<String>,
}

auth_message_extractor! {BinanceAuthMessageExtractor, BinanceAuthMessage(id:String)}

#[async_trait]
impl Transport<BinanceAuthMessage, BinanceAuthReply> for BinanceHttpTransport {
    // TODO
    fn new(_url: String) -> Self
    where
        Self: Sized,
    {
        BinanceHttpTransport
    }
    async fn send_and_wait_for_reply(
        &self,
        message: &BinanceAuthMessage,
        // TODO
        _timeout: chrono::Duration,
    ) -> StockTrekResult<BinanceAuthReply> {
        Ok(BinanceAuthReply {
            id: Some(message.id.clone()),
        })
    }
}

#[async_trait]
impl Transport<OrderRequestBody, BinanceOrderReply> for BinanceHttpTransport {
    fn new(_url: String) -> Self
    where
        Self: Sized,
    {
        BinanceHttpTransport
    }
    async fn send_and_wait_for_reply(
        &self,
        message: &OrderRequestBody,
        // TODO
        _timeout: chrono::Duration,
    ) -> StockTrekResult<BinanceOrderReply> {
        Ok(BinanceOrderReply {
            id: Some("".to_string()),
        })
    }
}

fn create_exchange_id() -> ExchangeId {
    ExchangeId("Binance".to_string())
}

fn create_capabilities() -> Vec<Capability> {
    vec![
        Capability::QuoteQuantity(QuoteQuantityCapability::AllowLimitPricing),
        Capability::QuoteQuantity(QuoteQuantityCapability::AllowTriggeredTiming),
        Capability::MultiLeg(MultiLegCapability::OneCancelsOther),
        Capability::MultiLeg(MultiLegCapability::OneTriggersOther),
        Capability::MultiLeg(MultiLegCapability::OneTriggersOco),
    ]
}

fn create_increments() -> HashMap<TradingPair, IncrementSizes> {
    IncrementSizesBuilder::new()
        .with(
            AssetId::bitcoin_native(),
            AssetId::bsc_usdt(),
            Decimal::from_i128_with_scale(1, 3),
            Decimal::from_i128_with_scale(1, 3),
        )
        .with(
            AssetId::bitcoin_native(),
            AssetId::tron_usdt(),
            Decimal::from_i128_with_scale(1, 3),
            Decimal::from_i128_with_scale(1, 3),
        )
        .with(
            AssetId::bitcoin_native(),
            AssetId::solana_usdt(),
            Decimal::from_i128_with_scale(1, 3),
            Decimal::from_i128_with_scale(1, 3),
        )
        .with(
            AssetId::bitcoin_native(),
            AssetId::polygon_usdt(),
            Decimal::from_i128_with_scale(1, 3),
            Decimal::from_i128_with_scale(1, 3),
        )
        .with(
            AssetId::bitcoin_native(),
            AssetId::ethereum_usdt(),
            Decimal::from_i128_with_scale(1, 3),
            Decimal::from_i128_with_scale(1, 3),
        )
        .build()
}

fn create_tickers() -> HashMap<AssetId, String> {
    let mut tickers = HashMap::new();
    tickers.insert(AssetId::aptos_native(), "APT".to_string());
    tickers.insert(AssetId::arbitrum_native(), "APT".to_string());
    tickers.insert(AssetId::arbitrum_usdc(), "APT".to_string());
    tickers.insert(AssetId::avalanche_native(), "APT".to_string());
    tickers.insert(AssetId::avalanche_usdc(), "APT".to_string());
    tickers.insert(AssetId::base_native(), "APT".to_string());
    tickers.insert(AssetId::base_usdc(), "APT".to_string());
    tickers.insert(AssetId::bitcoin_native(), "APT".to_string());
    tickers.insert(AssetId::bsc_native(), "APT".to_string());
    tickers.insert(AssetId::bsc_usdc(), "APT".to_string());
    tickers.insert(AssetId::bsc_usdt(), "APT".to_string());
    tickers.insert(AssetId::cosmos_native(), "APT".to_string());
    tickers.insert(AssetId::ethereum_native(), "APT".to_string());
    tickers.insert(AssetId::ethereum_reth(), "APT".to_string());
    tickers.insert(AssetId::ethereum_steth(), "APT".to_string());
    tickers.insert(AssetId::ethereum_usdc(), "APT".to_string());
    tickers.insert(AssetId::ethereum_usdt(), "APT".to_string());
    tickers.insert(AssetId::ethereum_wbtc(), "APT".to_string());
    tickers.insert(AssetId::kusama_native(), "APT".to_string());
    tickers.insert(AssetId::near_native(), "APT".to_string());
    tickers.insert(AssetId::optimism_native(), "APT".to_string());
    tickers.insert(AssetId::optimism_usdc(), "APT".to_string());
    tickers.insert(AssetId::polkadot_native(), "APT".to_string());
    tickers.insert(AssetId::polygon_native(), "APT".to_string());
    tickers.insert(AssetId::polygon_usdc(), "APT".to_string());
    tickers.insert(AssetId::polygon_usdt(), "APT".to_string());
    tickers.insert(AssetId::solana_native(), "APT".to_string());
    tickers.insert(AssetId::solana_usdc(), "APT".to_string());
    tickers.insert(AssetId::solana_usdt(), "APT".to_string());
    tickers.insert(AssetId::sui_native(), "APT".to_string());
    tickers.insert(AssetId::tron_native(), "APT".to_string());
    tickers.insert(AssetId::tron_usdt(), "APT".to_string());
    tickers
}

fn create_http_auth_spec() -> AuthSpec<
    BinanceState,
    BinanceCredentials,
    BinanceHttpTransports,
    BinanceHttpTransport,
    OrderRequestBody,
    BinanceOrderReply,
> {
    AuthSpec::<
        BinanceState,
        BinanceCredentials,
        BinanceHttpTransports,
        BinanceHttpTransport,
        OrderRequestBody,
        BinanceOrderReply,
    >::new(
        vec![AuthenticateLegImpl::<
            BinanceState,
            BinanceCredentials,
            BinanceHttpTransports,
            BinanceHttpTransport,
            BinanceAuthMessage,
            BinanceAuthReply,
        >::new(
            |t| &t.http,
            Duration::seconds(20),
            // TODO
            BinanceAuthMessageExtractor::new(|_s, _c, _t| "".to_string()),
            vec![StoreAuthValueImpl::new(
                |reply| Ok(reply.id.clone()),
                |state, value| state.id = value.clone(),
            )],
        )],
        MessageLeg::new(
            |t| &t.http,
            Duration::seconds(20),
            Extractors::new(
                SingleExtractor::new(
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| 1,
                ),
                OcoExtractor::new(
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| 1,
                ),
                OtoExtractor::new(
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| 1,
                ),
                OtOcoExtractor::new(
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| "".to_string(),
                    |o| 1,
                ),
            ),
            vec![OrderMessageSigner::<
                BinanceState,
                BinanceCredentials,
                OrderRequestBody,
            >::new(
                |c| &c.api_key,
                vec![],
                // TODO
                // |s, m| match m {
                //     Single(single) => vec![single.id.to_bytes()],
                //     _ => vec![],
                // },
                SigningAlgorithm::HmacSha256,
                ByteEncoding::HexUpper,
                |signature, message| message.signature = signature,
            )],
            OrderResponseExtractor::new(|response| {
                response.id.clone().unwrap_or("Missing".to_string())
            }),
        ),
    )
}

impl AdapterCreatorTrait<BinanceCredentials, BinanceHttpTransports> for BinanceHttpAdapterCreator {
    fn create_adapter(
        &self,
        credentials: BinanceCredentials,
        transports: BinanceHttpTransports,
    ) -> Adapter {
        let id = create_exchange_id();
        let capabilities = create_capabilities();
        let increments = create_increments();
        let tickers = create_tickers();
        let auth_spec = create_http_auth_spec();
        Adapter {
            id,
            capabilities,
            increments,
            symbol_ticker_divider: None,
            tickers,
            exchange_connector: ExchangeConnectorImpl::new(auth_spec, credentials, transports),
        }
    }
}

single_order_extractor! {
    apiKey: String,
    symbol: String,
    timestamp: i64,
    <BinanceState, BinanceCredentials>
    signature: String,
}

oco_order_extractor! {
    symbol: String,
    timestamp: i64,
    <BinanceState, BinanceCredentials>
    signature: String,
}

oto_order_extractor! {
    apiKey: String,
    symbol: String,
    timestamp: i64,
    <BinanceState, BinanceCredentials>
    signature: String,
}

otoco_order_extractor! {
    apiKey: String,
    symbol: String,
    timestamp: i64,
    <BinanceState, BinanceCredentials>
    signature: String,
}

order_request_extractor! {
    UnsignedSingleBody, UnsignedOcoBody, UnsignedOtoBody, UnsignedOtOcoBody,
    <BinanceState, BinanceCredentials>
    SignedSingleBody, SignedOcoBody, SignedOtoBody, SignedOtOcoBody,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceOrderSide {
    BUY,
    SELL,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceOrderType {
    LIMIT,
    MARKET,
    STOP_LOSS,
    STOP_LOSS_LIMIT,
    TAKE_PROFIT,
    TAKE_PROFIT_LIMIT,
    LIMIT_MAKER,
}

#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceTimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceSelfTradePreventionMode {
    EXPIRE_MAKER,
    EXPIRE_TAKER,
    EXPIRE_BOTH,
    NONE,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceCancelRestrictions {
    ONLY_NEW,
    ONLY_PARTIALLY_FILLED,
    NONE,
}

#[allow(non_camel_case_types)]
#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceWorkingType {
    MARK_PRICE,
    CONTRACT_PRICE,
}

#[derive(Debug, Display, Clone, Copy, Serialize)]
pub enum BinanceNewOrderResponseType {
    ACK,
    RESULT,
    FULL,
}
