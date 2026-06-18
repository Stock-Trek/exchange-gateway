use crate::{
    authenticate_leg::AuthenticateLeg,
    authenticator_creator::AuthenticatorCreatorTrait,
    cex::{
        cex_spec::CexSpec, increment_sizes::IncrementSizes, rate_limits_weights::RequestWeights,
    },
    connector::{Authenticator, ConnectorImpl},
    credentials::api_key_credential::ApiKeyCredentials,
    increments_leg::{IncrementsLeg, IncrementsLegImpl},
    message_leg::{MessageLeg, MessageLegImpl},
    messenger::MessengerImpl,
    sign::{
        convert_signer::ConvertSigner,
        signer::{Signer, SignerTrait},
    },
    transports::http_transport::{HttpMessageDto, HttpTransportTrait},
};
use async_trait::async_trait;
use bimap::BiMap;
use chrono::{Duration, Utc};
use p256::ecdsa::SigningKey;
use p256::ecdsa::signature::Signer as P256Signer;
use rust_decimal::Decimal;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use stock_trek::{
    cex::{
        asset_id::AssetId,
        capability::{CexCapability, QuoteQuantityCexCapability},
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

// ─── Outgoing message types ───────────────────────────────────────────────

/// The unsigned JSON body sent to Coinbase's POST /api/v3/brokerage/orders.
#[derive(Serialize)]
pub struct UnsignedMessageToCoinbase {
    pub product_id: String,
    pub side: Side,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    pub order_configuration: OrderConfiguration,
}

/// A signed (bearer-token) HTTP request to Coinbase.
#[derive(Serialize)]
pub struct SignedMessageToCoinbase {
    pub body: UnsignedMessageToCoinbase,
    pub bearer_token: String,
}

// ─── Order configuration types ────────────────────────────────────────────

/// Maps to the `order_configuration` field of the Coinbase create-order request.
/// Each variant corresponds to a supported order type.
#[derive(Serialize)]
#[serde(untagged)]
pub enum OrderConfiguration {
    MarketMarketIoc(MarketMarketIoc),
    LimitLimitGtc(LimitLimitGtc),
    StopLimitStopLimitGtc(StopLimitStopLimitGtc),
    StopMarketStopMarketGtc(StopMarketStopMarketGtc),
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct MarketMarketIoc {
    baseSize: Option<Decimal>,
    quoteSize: Option<Decimal>,
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct LimitLimitGtc {
    baseSize: Decimal,
    limitPrice: Decimal,
    postOnly: bool,
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct StopLimitStopLimitGtc {
    baseSize: Decimal,
    limitPrice: Decimal,
    stopPrice: Decimal,
    stopDirection: StopDirection,
}

#[allow(non_snake_case)]
#[derive(Serialize)]
pub struct StopMarketStopMarketGtc {
    baseSize: Decimal,
    stopPrice: Decimal,
    stopDirection: StopDirection,
}

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Serialize)]
pub enum StopDirection {
    STOP_DIRECTION_STOP_UP,
    STOP_DIRECTION_STOP_DOWN,
}

#[derive(Clone, Copy, Serialize)]
pub enum Side {
    BUY,
    SELL,
}

// ─── Response types ───────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct MessageFromCoinbase {
    pub success: bool,
    pub success_response: Option<SuccessResponse>,
    pub error_response: Option<ErrorResponse>,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct SuccessResponse {
    pub order_id: String,
    pub product_id: String,
    pub side: String,
    pub client_order_id: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    pub error_details: Option<String>,
    pub preview_failure_reason: Option<String>,
    pub new_order_failure_reason: Option<String>,
}

// ─── Products (increments) response types ─────────────────────────────────

#[derive(Clone, Serialize)]
pub struct UnsignedProductsMessage {
    pub bearer_token: String,
}

#[allow(non_snake_case)]
#[derive(Deserialize)]
pub struct ProductsResponse {
    pub products: Vec<ProductInfo>,
}

#[allow(non_snake_case, unused)]
#[derive(Deserialize)]
pub struct ProductInfo {
    pub product_id: String,
    pub base_increment: String,
    pub quote_increment: String,
    pub status: String,
}

// ─── Spec creator ─────────────────────────────────────────────────────────

pub struct CoinbaseRestSpecCreator {
    pub credentials: crate::credentials::api_key_credential::ApiKeyCredentials,
    pub transport: Arc<dyn HttpTransportTrait>,
}

impl
    AuthenticatorCreatorTrait<
        OrderRequest<AssetId, f64>,
        UnsignedMessageToCoinbase,
        SignedMessageToCoinbase,
        OrderResponse,
    > for CoinbaseRestSpecCreator
{
    fn into_authenticator(
        self,
    ) -> StockTrekResult<Authenticator<OrderRequest<AssetId, f64>, OrderResponse>> {
        let CoinbaseRestSpecCreator {
            credentials,
            transport,
        } = self;
        let authenticate_legs: Vec<
            AuthenticateLeg<UnsignedMessageToCoinbase, SignedMessageToCoinbase>,
        > = vec![authenticate_leg(&credentials)];
        let spec = CexSpec::new(
            capabilities(),
            request_weights(),
            tickers(),
            increments_leg(transport.clone()),
            authenticate_legs,
            message_leg(transport.clone(), &credentials),
        );
        Ok(ConnectorImpl::new(spec, initial_signer()))
    }
}

// ─── Capabilities / weights / tickers ─────────────────────────────────────

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
    tickers.insert(AssetId::aave(), "AAVE".to_string());
    tickers.insert(AssetId::arbitrum(), "ARB".to_string());
    tickers.insert(AssetId::avalanche(), "AVAX".to_string());
    tickers.insert(AssetId::bitcoin(), "BTC".to_string());
    tickers.insert(AssetId::bitcoin_cash(), "BCH".to_string());
    tickers.insert(AssetId::bnb(), "BNB".to_string());
    tickers.insert(AssetId::celo(), "CELO".to_string());
    tickers.insert(AssetId::cosmos(), "ATOM".to_string());
    tickers.insert(AssetId::cronos(), "CRO".to_string());
    tickers.insert(AssetId::dai(), "DAI".to_string());
    tickers.insert(AssetId::dogecoin(), "DOGE".to_string());
    tickers.insert(AssetId::ethereum(), "ETH".to_string());
    tickers.insert(AssetId::fantom(), "FTM".to_string());
    tickers.insert(AssetId::gnosis(), "GNO".to_string());
    tickers.insert(AssetId::link(), "LINK".to_string());
    tickers.insert(AssetId::litecoin(), "LTC".to_string());
    tickers.insert(AssetId::moonbeam(), "GLMR".to_string());
    tickers.insert(AssetId::near(), "NEAR".to_string());
    tickers.insert(AssetId::optimism(), "OP".to_string());
    tickers.insert(AssetId::osmosis(), "OSMO".to_string());
    tickers.insert(AssetId::polygon(), "POL".to_string());
    tickers.insert(AssetId::solana(), "SOL".to_string());
    tickers.insert(AssetId::tron(), "TRX".to_string());
    tickers.insert(AssetId::uni(), "UNI".to_string());
    tickers.insert(AssetId::usdc(), "USDC".to_string());
    tickers.insert(AssetId::usdt(), "USDT".to_string());
    tickers.insert(AssetId::wbtc(), "WBTC".to_string());
    tickers.insert(AssetId::weth(), "WETH".to_string());
    tickers
}

// ─── Increments leg ───────────────────────────────────────────────────────

fn increments_leg(transport: Arc<dyn HttpTransportTrait>) -> IncrementsLeg {
    let timeout = Duration::seconds(30);
    let messenger = MessengerImpl::new(
        transport,
        timeout,
        products_dto,
        deserialize_products,
        filter_products,
    );
    IncrementsLegImpl::new(products_message(), messenger, to_increments)
}

fn products_message() -> UnsignedProductsMessage {
    UnsignedProductsMessage {
        bearer_token: String::new(),
    }
}

fn products_dto(message: &UnsignedProductsMessage) -> StockTrekResult<HttpMessageDto> {
    let mut headers = HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        format!("Bearer {}", message.bearer_token),
    );
    Ok(HttpMessageDto {
        headers,
        body_json: String::new(),
    })
}

fn deserialize_products(dto: HttpMessageDto) -> StockTrekResult<ProductsResponse> {
    let response: ProductsResponse =
        serde_json::from_str(dto.body_json.as_str()).map_err(|_e| {
            StockTrekError::General(GeneralError::Message(
                "Failed to deserialize products response".to_string(),
            ))
        })?;
    Ok(response)
}

fn filter_products(response: ProductsResponse) -> StockTrekResult<ProductsResponse> {
    Ok(response)
}

fn to_increments(response: ProductsResponse) -> HashMap<TradingPair, IncrementSizes> {
    let tickers = tickers();
    let mut increments = HashMap::new();
    for product in response.products {
        // Product ID format: "BTC-USD"
        let parts: Vec<&str> = product.product_id.split('-').collect();
        if parts.len() != 2 {
            continue;
        }
        let base_name = parts[0];
        let quote_name = parts[1];
        let base = tickers.get_by_right(&base_name.to_string());
        let quote = tickers.get_by_right(&quote_name.to_string());
        if let Some(base) = base
            && let Some(quote) = quote
        {
            let trading_pair = TradingPair::new(base.clone(), quote.clone());
            // Parse decimal increments
            if let (Ok(base_inc), Ok(quote_inc)) = (
                Decimal::from_str_exact(&product.base_increment),
                Decimal::from_str_exact(&product.quote_increment),
            ) {
                let increment_sizes = IncrementSizes::new(base_inc, quote_inc);
                increments.insert(trading_pair, increment_sizes);
            }
        }
    }
    increments
}

// ─── Message leg (order placement) ────────────────────────────────────────

fn message_leg(
    transport: Arc<dyn HttpTransportTrait>,
    credentials: &crate::credentials::api_key_credential::ApiKeyCredentials,
) -> MessageLeg<
    OrderRequest<AssetId, Decimal>,
    UnsignedMessageToCoinbase,
    SignedMessageToCoinbase,
    OrderResponse,
> {
    let _ = credentials;
    let timeout = Duration::seconds(10);
    let messenger = MessengerImpl::new(
        transport,
        timeout,
        dto,
        deserialize_reply,
        filter_reply_order_placed,
    );
    MessageLegImpl::<
        OrderRequest<AssetId, Decimal>,
        UnsignedMessageToCoinbase,
        SignedMessageToCoinbase,
        OrderResponse,
    >::new(trade_request_to_message, messenger)
}

fn trade_request_to_message(
    _preferences: &Preferences,
    tickers: &BiMap<AssetId, String>,
    order_request: OrderRequest<AssetId, Decimal>,
) -> StockTrekResult<UnsignedMessageToCoinbase> {
    match order_request {
        OrderRequest::Single(single_order_request) => {
            let base = tickers.get_by_left(&single_order_request.base);
            let quote = tickers.get_by_left(&single_order_request.quote);
            if let Some(base) = base
                && let Some(quote) = quote
            {
                let side = match single_order_request.side {
                    OrderSide::Buy => Side::BUY,
                    OrderSide::Sell => Side::SELL,
                };
                let product_id = format!("{}-{}", base, quote);
                let order_configuration = coinbase_order_configuration(&single_order_request)?;
                Ok(UnsignedMessageToCoinbase {
                    product_id,
                    side,
                    client_order_id: Some(single_order_request.order_tag.0),
                    order_configuration,
                })
            } else {
                Err(StockTrekError::General(GeneralError::Message(
                    "Failed to find ticker for base or quote".to_string(),
                )))
            }
        }
    }
}

fn coinbase_order_configuration(
    order: &stock_trek::cex::orders::single::SingleOrderGeneric<AssetId, Decimal>,
) -> StockTrekResult<OrderConfiguration> {
    match order.activation {
        OrderActivation::Immediate => match order.pricing {
            OrderPricing::Market => match order.quantity {
                OrderQuantity::OfBase(base_size) => {
                    Ok(OrderConfiguration::MarketMarketIoc(MarketMarketIoc {
                        baseSize: Some(base_size),
                        quoteSize: None,
                    }))
                }
                OrderQuantity::OfQuote(quote_size) => {
                    Ok(OrderConfiguration::MarketMarketIoc(MarketMarketIoc {
                        baseSize: None,
                        quoteSize: Some(quote_size),
                    }))
                }
            },
            OrderPricing::Limit {
                price,
                time_in_force,
            } => match time_in_force {
                OrderTimeInForce::GoodTillCancelled => {
                    let base_size = match order.quantity {
                        OrderQuantity::OfBase(q) => q,
                        OrderQuantity::OfQuote(_) => {
                            return Err(StockTrekError::General(GeneralError::Message(
                                "Coinbase limit GTC orders require base size".to_string(),
                            )));
                        }
                    };
                    Ok(OrderConfiguration::LimitLimitGtc(LimitLimitGtc {
                        baseSize: base_size,
                        limitPrice: price,
                        postOnly: false,
                    }))
                }
                OrderTimeInForce::FillOrKill => match order.quantity {
                    OrderQuantity::OfBase(base_size) => {
                        Ok(OrderConfiguration::MarketMarketIoc(MarketMarketIoc {
                            baseSize: Some(base_size),
                            quoteSize: None,
                        }))
                    }
                    OrderQuantity::OfQuote(quote_size) => {
                        Ok(OrderConfiguration::MarketMarketIoc(MarketMarketIoc {
                            baseSize: None,
                            quoteSize: Some(quote_size),
                        }))
                    }
                },
                OrderTimeInForce::ImmediateOrCancel => {
                    Err(StockTrekError::General(GeneralError::Message(
                        "Coinbase does not support IOC limit orders directly; use SOR_LIMIT_IOC"
                            .to_string(),
                    )))
                }
            },
        },
        OrderActivation::PriceTriggered {
            activation_price,
            direction,
            ..
        }
        | OrderActivation::Trailing {
            activation_price,
            direction,
            ..
        } => {
            let stop_direction = match direction {
                OrderTriggerDirection::Above => StopDirection::STOP_DIRECTION_STOP_UP,
                OrderTriggerDirection::Below => StopDirection::STOP_DIRECTION_STOP_DOWN,
            };
            match order.pricing {
                OrderPricing::Market => {
                    let base_size = match order.quantity {
                        OrderQuantity::OfBase(q) => q,
                        OrderQuantity::OfQuote(_) => {
                            return Err(StockTrekError::General(GeneralError::Message(
                                "Coinbase stop market orders require base size".to_string(),
                            )));
                        }
                    };
                    Ok(OrderConfiguration::StopMarketStopMarketGtc(
                        StopMarketStopMarketGtc {
                            baseSize: base_size,
                            stopPrice: activation_price,
                            stopDirection: stop_direction,
                        },
                    ))
                }
                OrderPricing::Limit { price, .. } => {
                    let base_size = match order.quantity {
                        OrderQuantity::OfBase(q) => q,
                        OrderQuantity::OfQuote(_) => {
                            return Err(StockTrekError::General(GeneralError::Message(
                                "Coinbase stop limit orders require base size".to_string(),
                            )));
                        }
                    };
                    Ok(OrderConfiguration::StopLimitStopLimitGtc(
                        StopLimitStopLimitGtc {
                            baseSize: base_size,
                            limitPrice: price,
                            stopPrice: activation_price,
                            stopDirection: stop_direction,
                        },
                    ))
                }
            }
        }
    }
}

// ─── DTO / serialization helpers ──────────────────────────────────────────

fn dto(message: &SignedMessageToCoinbase) -> StockTrekResult<HttpMessageDto> {
    let body_json = serde_json::to_string(&message.body).map_err(|_e| {
        StockTrekError::General(GeneralError::Message(
            "Failed to serialize Coinbase order message".to_string(),
        ))
    })?;
    let mut headers = HashMap::new();
    headers.insert(
        "Authorization".to_string(),
        format!("Bearer {}", message.bearer_token),
    );
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    Ok(HttpMessageDto { headers, body_json })
}

fn deserialize_reply(dto: HttpMessageDto) -> StockTrekResult<MessageFromCoinbase> {
    let message: MessageFromCoinbase =
        serde_json::from_str(dto.body_json.as_str()).map_err(|_e| {
            StockTrekError::General(GeneralError::Message(
                "Failed to deserialize Coinbase response".to_string(),
            ))
        })?;
    Ok(message)
}

fn filter_reply_order_placed(reply: MessageFromCoinbase) -> StockTrekResult<OrderResponse> {
    if reply.success {
        if let Some(success) = reply.success_response {
            Ok(OrderResponse {
                tag: OrderTag(success.client_order_id),
            })
        } else {
            Err(StockTrekError::General(GeneralError::Message(
                "Coinbase order succeeded but no success_response".to_string(),
            )))
        }
    } else {
        let error_message = reply
            .error_response
            .map(|e| e.message)
            .unwrap_or_else(|| "Unknown Coinbase error".to_string());
        Err(StockTrekError::General(GeneralError::Message(
            error_message,
        )))
    }
}

// ─── JWT authentication ───────────────────────────────────────────────────

/// The unsigned JWT payload for Coinbase Cloud API authentication.
/// https://docs.cdp.coinbase.com/advanced-trade/docs/rest-api-auth
#[derive(Serialize)]
pub struct UnsignedJwtForCoinbase {
    sub: String,
    iss: String,
    #[serde(rename = "aud")]
    aud: Vec<String>,
    iat: i64,
    exp: i64,
}

/// The full JWT structure before signing (header + payload as base64url-encoded segments).
pub struct UnsignedJwtMessage {
    pub jwt_unsigned: String, // "header.payload" in base64url
}

/// The signed JWT bearer token.
pub struct JwtToken {
    pub token: String,
}

/// Authenticate leg for Coinbase REST API.
/// Generates a JWT bearer token using the Cloud API credentials (ECDSA P-256 key),
/// then creates a signer that embeds this token into all subsequent messages.
fn authenticate_leg(
    credentials: &ApiKeyCredentials,
) -> AuthenticateLeg<UnsignedMessageToCoinbase, SignedMessageToCoinbase> {
    let api_key = credentials.api_key.clone();
    let secret = credentials.secret.clone();

    // Build the signing key from the secret (ECDSA P-256 private key bytes)
    let signing_key = SigningKey::from_slice(secret.expose_secret().as_bytes())
        .expect("Failed to create Coinbase ECDSA P-256 signing key");

    Box::new(CoinbaseJwtAuthenticateLeg {
        api_key,
        signing_key,
        _secret: secret,
    })
}

struct CoinbaseJwtAuthenticateLeg {
    api_key: String,
    signing_key: SigningKey,
    _secret: secrecy::SecretString,
}

#[async_trait]
impl
    crate::authenticate_leg::AuthenticateLegTrait<
        UnsignedMessageToCoinbase,
        SignedMessageToCoinbase,
    > for CoinbaseJwtAuthenticateLeg
{
    async fn do_leg(
        &self,
        _signer: &Signer<UnsignedMessageToCoinbase, SignedMessageToCoinbase>,
    ) -> StockTrekResult<Signer<UnsignedMessageToCoinbase, SignedMessageToCoinbase>> {
        let now = Utc::now().timestamp();
        let payload = UnsignedJwtForCoinbase {
            sub: self.api_key.clone(),
            iss: "coinbase-cloud".to_string(),
            aud: vec!["rest.coinbase.com".to_string()],
            iat: now,
            exp: now + 120,
        };

        // Build JWT header: {"alg":"ES256","kid":"<api_key>","typ":"JWT"}
        let header = serde_json::json!({
            "alg": "ES256",
            "kid": self.api_key,
            "typ": "JWT",
        });

        // Base64url-encode header and payload
        let header_b64 = base64url_encode(&serde_json::to_vec(&header).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "Failed to serialize JWT header: {e}"
            )))
        })?);
        let payload_b64 = base64url_encode(&serde_json::to_vec(&payload).map_err(|e| {
            StockTrekError::General(GeneralError::Message(format!(
                "Failed to serialize JWT payload: {e}"
            )))
        })?);

        // Sign the "header.payload" string using raw ECDSA P-256 (ES256)
        let signing_input = format!("{header_b64}.{payload_b64}");
        let signature: p256::ecdsa::Signature = self.signing_key.sign(signing_input.as_bytes());
        // ES256 uses raw R||S format (64 bytes); to_bytes() gives the fixed-size big-endian format
        let signature_b64 = base64url_encode(&signature.to_vec());

        let jwt = format!("{signing_input}.{signature_b64}");

        // Create a new signer that embeds this JWT as bearer token
        let signer = CoinbaseJwtSigner { bearer_token: jwt };
        Ok(Box::new(signer))
    }
}

/// Signer that wraps unsigned messages with a JWT bearer token.
struct CoinbaseJwtSigner {
    bearer_token: String,
}

impl SignerTrait<UnsignedMessageToCoinbase, SignedMessageToCoinbase> for CoinbaseJwtSigner {
    fn sign(
        &self,
        unsigned: UnsignedMessageToCoinbase,
    ) -> StockTrekResult<SignedMessageToCoinbase> {
        Ok(SignedMessageToCoinbase {
            body: unsigned,
            bearer_token: self.bearer_token.clone(),
        })
    }
}

/// Base64url-encode bytes (no padding, URL-safe).
fn base64url_encode(data: &[u8]) -> String {
    data_encoding::BASE64URL_NOPAD.encode(data)
}

/// Initial no-op signer used before the authentication leg runs.
fn initial_signer() -> Signer<UnsignedMessageToCoinbase, SignedMessageToCoinbase> {
    ConvertSigner::new(|unsigned| SignedMessageToCoinbase {
        body: unsigned,
        bearer_token: String::new(),
    })
}
