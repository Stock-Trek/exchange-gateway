use crate::{
    authenticate_leg::AuthenticateLegImpl,
    cex::cex_spec::CexSpec,
    credentials::api_key_credential::ApiKeyCredentials,
    exchange_spec::ExchangeSpec,
    increment_sizes::IncrementSizesBuilder,
    message_leg::MessageLegImpl,
    spec_creator::SpecCreatorTrait,
    transports::{
        http_transport::{HttpMessageDto, HttpTransportTrait},
        transport::TransportTrait,
    },
};
use async_trait::async_trait;
use chrono::Duration;
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    cex::{
        asset_id::AssetId,
        capability::{CexCapability, MultiLegCexCapability, QuoteQuantityCexCapability},
        cex_id::CexId,
        order_id::OrderId,
        order_request::OrderRequest,
        order_response::OrderResponse,
    },
    error::result::StockTrekResult,
};

pub struct BinanceState {
    pub id: Option<String>,
}

pub struct BinanceCredentials {
    pub api_key: ApiKeyCredentials,
}

pub struct BinanceHttpTransports {
    pub http: ReqwestHttpTransport,
}

pub struct ReqwestHttpTransport;

pub struct BinanceAuthReply {
    pub id: Option<String>,
}

pub struct BinanceOrderReply {
    pub id: Option<String>,
    pub symbol: Option<String>,
}

pub struct SingleOrderMessage {
    pub symbol: String,
    pub timestamp: i64,
    pub signature: String,
}

pub struct OcoOrderMessage {
    pub symbol: String,
    pub timestamp: i64,
    pub signature: String,
}

pub struct OtoOrderMessage {
    pub symbol: String,
    pub timestamp: i64,
    pub signature: String,
}

pub struct OtOcoOrderMessage {
    pub symbol: String,
    pub timestamp: i64,
    pub signature: String,
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

impl HttpTransportTrait for ReqwestHttpTransport {}

#[async_trait]
impl TransportTrait for ReqwestHttpTransport {
    type MessageDto = HttpMessageDto;
    type TransportMessage = HttpMessageDto;
    async fn send(
        &self,
        message_dto: Self::MessageDto,
        _timeout: Duration,
    ) -> StockTrekResult<Self::MessageDto> {
        Ok(message_dto)
    }
}

pub struct BinanceHttpSpecCreator;

impl
    SpecCreatorTrait<
        BinanceHttpTransports,
        BinanceCredentials,
        BinanceState,
        OrderRequest<AssetId, f64>,
        OrderResponse,
    > for BinanceHttpSpecCreator
{
    fn create_spec(
        &self,
    ) -> ExchangeSpec<
        BinanceHttpTransports,
        BinanceCredentials,
        BinanceState,
        OrderRequest<AssetId, f64>,
        OrderResponse,
    > {
        let id = CexId("Binance".to_string());
        let capabilities = vec![
            CexCapability::QuoteQuantity(QuoteQuantityCexCapability::AllowLimitPricing),
            CexCapability::QuoteQuantity(QuoteQuantityCexCapability::AllowTriggeredTiming),
            CexCapability::MultiLeg(MultiLegCexCapability::OneCancelsOther),
            CexCapability::MultiLeg(MultiLegCexCapability::OneTriggersOther),
            CexCapability::MultiLeg(MultiLegCexCapability::OneTriggersOco),
        ];
        let increments = IncrementSizesBuilder::new()
            .with(
                AssetId::bitcoin(),
                AssetId::usdc(),
                Decimal::from_i128_with_scale(1, 3),
                Decimal::from_i128_with_scale(1, 3),
            )
            .build();
        let mut tickers = HashMap::new();
        tickers.insert(AssetId::usdc(), "USDC".to_string());
        tickers.insert(AssetId::bitcoin(), "BTC".to_string());
        let authenticate_legs = vec![AuthenticateLegImpl::<
            BinanceHttpTransports,
            BinanceCredentials,
            BinanceState,
            ReqwestHttpTransport,
        >::new(
            |t| &t.http,
            Duration::seconds(20),
            |_t, _c, _s| HttpMessageDto {
                headers: HashMap::new(),
                body_json: "{}".to_string(),
            },
            |m, _s| {
                let state: BinanceState = BinanceState {
                    id: Some(
                        m.headers
                            .get("dhsjkfhj")
                            .unwrap_or(&"fds".to_string())
                            .clone(),
                    ),
                };
                Ok(state)
            },
        )];
        let message_leg = MessageLegImpl::<
            BinanceHttpTransports,
            BinanceCredentials,
            BinanceState,
            OrderRequest<AssetId, Decimal>,
            OrderResponse,
            ReqwestHttpTransport,
        >::new(
            |t| &t.http,
            Duration::seconds(20),
            |_c, _s, order_request| {
                let body = match order_request {
                    OrderRequest::Single(_single) => "",
                    OrderRequest::OneCancelsOther(_oco) => "",
                    OrderRequest::OneTriggersOther(_oto) => "",
                    OrderRequest::OneTriggersOco(_otoco) => "",
                };
                Ok(HttpMessageDto {
                    headers: HashMap::new(),
                    body_json: body.to_string(),
                })
            },
            |_r| {
                Ok(OrderResponse {
                    id: OrderId("".to_string()),
                })
            },
        );
        CexSpec::new(id, capabilities, increments, authenticate_legs, message_leg)
    }
}
