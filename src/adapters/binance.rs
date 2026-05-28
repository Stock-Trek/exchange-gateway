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
    message_leg::MessageLegImpl,
    transport::{
        http_transport::{HttpMessageDto, HttpTransportTrait},
        transport::TransportTrait,
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
    order::{order_id::OrderId, order_request::OrderRequest, order_response::OrderResponse},
};

pub struct BinanceHttpAdapterCreator;

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

impl Destroy for BinanceCredentials {
    fn destroy(&mut self) {}
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

fn create_http_protocol()
-> ExchangeProtocol<BinanceHttpTransports, BinanceCredentials, BinanceState> {
    ExchangeProtocol::<BinanceHttpTransports, BinanceCredentials, BinanceState>::new(
        vec![AuthenticateLegImpl::<
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
            |m, s| {
                s.id = Some(
                    m.headers
                        .get("dhsjkfhj")
                        .unwrap_or(&"fds".to_string())
                        .clone(),
                );
                Ok(())
            },
        )],
        MessageLegImpl::new(
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
            exchange_connector: ExchangeConnectorImpl::new(protocol, transports, credentials),
        }
    }
}
