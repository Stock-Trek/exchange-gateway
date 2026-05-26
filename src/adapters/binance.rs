use crate::{
    adapt::{
        adapter::Adapter, adapter_creator::AdapterCreatorTrait,
        increment_sizes::IncrementSizesBuilder,
    },
    authenticate_leg::AuthenticateLegImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    destroy::Destroy,
    exchange_connector::ExchangeConnectorImpl,
    exchange_protocol::ExchangeProtocol,
    message_leg::MessageLeg,
    sign::{encode::byte_encoding::ByteEncoding, encrypt::signing_algorithm::SigningAlgorithm},
    transport::transport::Transport,
    values::{
        auth_message::auth_message, order_response::OrderResponseExtractor,
        signed_order_request::signed_order_request, signed_order_variant::signed_order_variant,
        signer::SignatureGenerator, store_auth_value::StoreAuthValueImpl,
    },
};
use async_trait::async_trait;
use chrono::Duration;
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    asset_id::AssetId,
    capability::{Capability, MultiLegCapability, QuoteQuantityCapability},
    error::result::StockTrekResult,
    exchange_id::ExchangeId,
};

pub struct BinanceHttpAdapterCreator;

pub struct BinanceState {
    pub id: Option<String>,
}

pub struct BinanceCredentials {
    pub api_key: ApiKeyCredentials,
}

pub struct BinanceHttpTransports {
    pub http: BinanceHttpTransport,
}

pub struct BinanceHttpTransport;

pub struct BinanceAuthReply {
    pub id: Option<String>,
}

pub struct BinanceOrderReply {
    pub id: Option<String>,
    pub symbol: Option<String>,
}

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

#[async_trait]
impl Transport<AuthMessage, BinanceAuthReply> for BinanceHttpTransport {
    // TODO
    fn new(_url: String) -> Self
    where
        Self: Sized,
    {
        BinanceHttpTransport
    }
    async fn send_and_wait_for_reply(
        &self,
        message: &AuthMessage,
        // TODO
        _timeout: chrono::Duration,
    ) -> StockTrekResult<BinanceAuthReply> {
        Ok(BinanceAuthReply {
            id: Some(message.id.clone()),
        })
    }
}

#[async_trait]
impl Transport<SignedOrderRequestMessage, BinanceOrderReply> for BinanceHttpTransport {
    fn new(_url: String) -> Self
    where
        Self: Sized,
    {
        BinanceHttpTransport
    }
    async fn send_and_wait_for_reply(
        &self,
        // TODO
        _message: &SignedOrderRequestMessage,
        _timeout: chrono::Duration,
    ) -> StockTrekResult<BinanceOrderReply> {
        Ok(BinanceOrderReply {
            id: Some("".to_string()),
            symbol: Some("".to_string()),
        })
    }
}

fn create_http_protocol() -> ExchangeProtocol<
    BinanceState,
    BinanceCredentials,
    BinanceHttpTransports,
    BinanceHttpTransport,
    SignedOrderRequestMessage,
    BinanceOrderReply,
> {
    ExchangeProtocol::<
        BinanceState,
        BinanceCredentials,
        BinanceHttpTransports,
        BinanceHttpTransport,
        SignedOrderRequestMessage,
        BinanceOrderReply,
    >::new(
        vec![AuthenticateLegImpl::<
            BinanceState,
            BinanceCredentials,
            BinanceHttpTransports,
            BinanceHttpTransport,
            AuthMessage,
            BinanceAuthReply,
        >::new(
            |t| &t.http,
            Duration::seconds(20),
            // TODO
            AuthMessageExtractorImpl::new(|_s, _c, _t| "".to_string()),
            vec![StoreAuthValueImpl::new(
                |reply| Ok(reply.id.clone()),
                |state, value| state.id = value.clone(),
            )],
        )],
        MessageLeg::new(
            |t| &t.http,
            Duration::seconds(20),
            SignedOrderRequestExtractor::new(
                single::SignedOrderExtractor::new(
                    single::UnsignedOrderFieldExtractors::new(|o| o.base.to_string(), |_o| 123),
                    single::SignedOrderFieldExtractors::new(SignatureGenerator::<
                        BinanceState,
                        BinanceCredentials,
                        single::UnsignedOrderMessage,
                    >::new(
                        |c| &c.api_key,
                        vec![|_s, u| Some(u.symbol.to_string().into_bytes())],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
                oco::SignedOrderExtractor::new(
                    oco::UnsignedOrderFieldExtractors::new(
                        |o| o.primary.base.to_string(),
                        |_o| 123,
                    ),
                    oco::SignedOrderFieldExtractors::new(SignatureGenerator::new(
                        |c| &c.api_key,
                        vec![],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
                oto::SignedOrderExtractor::new(
                    oto::UnsignedOrderFieldExtractors::new(
                        |o| o.primary.base.to_string(),
                        |_o| 123,
                    ),
                    oto::SignedOrderFieldExtractors::new(SignatureGenerator::new(
                        |c| &c.api_key,
                        vec![],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
                otoco::SignedOrderExtractor::new(
                    otoco::UnsignedOrderFieldExtractors::new(
                        |o| o.primary.base.to_string(),
                        |_o| 123,
                    ),
                    otoco::SignedOrderFieldExtractors::new(SignatureGenerator::new(
                        |c| &c.api_key,
                        vec![],
                        SigningAlgorithm::HmacSha256,
                        ByteEncoding::Base64,
                    )),
                ),
            ),
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
        let id = ExchangeId("Binance".to_string());
        let capabilities = vec![
            Capability::QuoteQuantity(QuoteQuantityCapability::AllowLimitPricing),
            Capability::QuoteQuantity(QuoteQuantityCapability::AllowTriggeredTiming),
            Capability::MultiLeg(MultiLegCapability::OneCancelsOther),
            Capability::MultiLeg(MultiLegCapability::OneTriggersOther),
            Capability::MultiLeg(MultiLegCapability::OneTriggersOco),
        ];
        let increments = IncrementSizesBuilder::new()
            .with(
                AssetId::bitcoin_native(),
                AssetId::base_usdc(),
                Decimal::from_i128_with_scale(1, 3),
                Decimal::from_i128_with_scale(1, 3),
            )
            .build();
        let mut tickers = HashMap::new();
        tickers.insert(AssetId::base_usdc(), "APT".to_string());
        tickers.insert(AssetId::bitcoin_native(), "APT".to_string());
        let protocol = create_http_protocol();
        Adapter {
            id,
            capabilities,
            increments,
            symbol_ticker_divider: None,
            tickers,
            exchange_connector: ExchangeConnectorImpl::new(protocol, credentials, transports),
        }
    }
}

auth_message! {
    <BinanceState, BinanceCredentials, BinanceHttpTransport>,
    id: String,
}

signed_order_variant! {
    single,
    ::stock_trek::order::orders::single::SingleOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::BinanceState, crate::adapters::binance::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_variant! {
    oco,
    ::stock_trek::order::orders::one_cancels_other::OneCancelsOtherOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::BinanceState, crate::adapters::binance::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_variant! {
    oto,
    ::stock_trek::order::orders::one_triggers_other::OneTriggersOtherOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::BinanceState, crate::adapters::binance::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_variant! {
    otoco,
    ::stock_trek::order::orders::one_triggers_oco::OneTriggersOcoOrderGeneric<::stock_trek::prelude::AssetId, ::rust_decimal::Decimal>,
    <crate::adapters::binance::BinanceState, crate::adapters::binance::BinanceCredentials>,
    (
        symbol: String,
        timestamp: i64,
    ),
    signature,
}

signed_order_request! {
    <BinanceState, BinanceCredentials>,
    single::SignedOrderMessage: single::SignedOrderExtractor,
    oco::SignedOrderMessage: oco::SignedOrderExtractor,
    oto::SignedOrderMessage: oto::SignedOrderExtractor,
    otoco::SignedOrderMessage: otoco::SignedOrderExtractor,
}
