use crate::{
    authenticator::{AuthenticateLeg, Authenticator, AuthenticatorImpl, IncrementsLeg},
    authenticator_creator::AuthenticatorCreator,
    connector::{RateLimits, RequestWeights},
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{SignatureAppender, TryConvertFromRequest, TryConvertToResponse},
    messenger::MessengerImpl,
    rate_limit::{multi_rate_limiter::MultiRateLimiter, rate_limit_config::RateLimitConfig},
    sign::{
        convert_signer::ConvertSigner, encode::byte_encoding::ByteEncoding,
        encrypt::signing_algorithm::SigningAlgorithm, message_signer::MessageSigner,
        signer::Signer,
    },
    transports::websocket_transport::{WebsocketMessageDto, WebsocketTransportTrait},
};
use chrono::{Duration, Utc};
use exchange_types::binance::{
    exchange_info::BinanceExchangeInfoParams,
    logon::BinanceLogonParams,
    signed::{BinanceParams, BinanceSignature, BinanceUnsignedParams},
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
};
use std::marker::PhantomData;
use uuid::Uuid;

pub struct BinanceWebsocketAuthenticatorCreator<TTransport, TRequest, TResponse> {
    pub(crate) transport: TTransport,
    pub(crate) use_session: bool,
    pub(crate) to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
    pub(crate) connector_timeout: Duration,
    pub(crate) _phantom_request: PhantomData<TRequest>,
}

impl<TTransport, TRequest, TResponse>
    AuthenticatorCreator<TRequest, BinanceWebsocketUnsignedRequest, ApiKeyCredentials, TResponse>
    for BinanceWebsocketAuthenticatorCreator<TTransport, TRequest, TResponse>
where
    TTransport: WebsocketTransportTrait + 'static,
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    fn into_authenticator(
        self,
    ) -> Authenticator<TRequest, BinanceWebsocketUnsignedRequest, ApiKeyCredentials, TResponse>
    {
        let BinanceWebsocketAuthenticatorCreator {
            transport,
            to_response,
            use_session,
            connector_timeout,
            _phantom_request,
        } = self;
        let messenger = Box::new(MessengerImpl::new(transport, dto, from_dto));
        let increments_leg = increments_leg();
        let authenticate_legs = if use_session {
            vec![authenticate_leg()]
        } else {
            vec![]
        };
        Box::new(AuthenticatorImpl {
            messenger,
            increments_leg,
            to_response,
            authenticate_legs,
            connector_timeout,
            create_signer_from_credentials,
            rate_limits: rate_limits(),
            request_weights: request_weights(),
            _phantom_request,
        })
    }
}

pub fn unsigned_message_converter<TRequest>(
    to_unsigned_binance_params: TryConvertFromRequest<TRequest, BinanceUnsignedParams>,
) -> TryConvertFromRequest<TRequest, BinanceWebsocketUnsignedRequest>
where
    TRequest: 'static,
{
    Box::new(
        move |request| -> EGResult<BinanceWebsocketUnsignedRequest> {
            let id = id();
            let method = BinanceWebsocketMethodName::PlaceOrder;
            let params = to_unsigned_binance_params(request)?;
            Ok(BinanceWebsocketUnsignedRequest {
                metadata: BinanceWebsocketMetadata { id, method },
                params,
            })
        },
    )
}

fn increments_leg() -> IncrementsLeg<BinanceWebsocketRequest> {
    let timeout = Duration::seconds(30);
    let message = increments_message();
    IncrementsLeg { message, timeout }
}

fn authenticate_leg() -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let timeout = Duration::seconds(20);
    AuthenticateLeg {
        create_auth_message,
        create_signer_from: create_signer_from_message,
        timeout,
    }
}

fn increments_message() -> BinanceWebsocketRequest {
    let id = id();
    BinanceWebsocketRequest {
        metadata: BinanceWebsocketMetadata {
            id,
            method: BinanceWebsocketMethodName::ExchangeInfo,
        },
        params: BinanceParams {
            signature: None,
            params: BinanceUnsignedParams::ExchangeInfo(BinanceExchangeInfoParams {
                permissions: vec!["SPOT".to_string()],
                symbolStatus: "TRADING".to_string(),
            }),
        },
    }
}

fn create_auth_message() -> BinanceWebsocketUnsignedRequest {
    let id = id();
    let timestamp = Utc::now().timestamp();
    let params = BinanceLogonParams { timestamp };
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id,
            method: BinanceWebsocketMethodName::Logon,
        },
        params: BinanceUnsignedParams::Logon(params),
    }
}

fn unsigned_message_bytes(message: &BinanceWebsocketUnsignedRequest) -> EGResult<Vec<u8>> {
    Ok(serde_urlencoded::to_string(message).unwrap().into_bytes())
}

fn dto(message: &BinanceWebsocketRequest) -> EGResult<WebsocketMessageDto> {
    let body_json =
        serde_json::to_string(&message).map_err(|_e| EGError::Custom("".to_string()))?;
    Ok(WebsocketMessageDto { body_json })
}

fn from_dto(dto: WebsocketMessageDto) -> EGResult<BinanceWebsocketResponse> {
    let message: BinanceWebsocketResponse = serde_json::from_str(dto.body_json.as_str())
        .map_err(|_e| EGError::Custom("Failed to deserialize response".to_string()))?;
    Ok(message)
}

fn create_signer_from_credentials(
    credentials: ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    let data_signer = SigningAlgorithm::Ed25519
        .signer(&secret)
        .map_err(|_e| EGError::Custom("Cannot create signer".to_string()))?;
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        unsigned_message_bytes,
        data_signer,
        ByteEncoding::Base64,
        signature_appender(api_key),
    )))
}

fn create_signer_from_message(
    _message: BinanceWebsocketResponse,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    Ok(Box::new(ConvertSigner::new(converter)))
}

fn converter(unsigned: BinanceWebsocketUnsignedRequest) -> EGResult<BinanceWebsocketRequest> {
    let BinanceWebsocketUnsignedRequest { metadata, params } = unsigned;
    let params = BinanceParams {
        signature: None,
        params,
    };
    Ok(BinanceWebsocketRequest { metadata, params })
}

// TODO ensure this is correct
fn rate_limits() -> RateLimits {
    let one_minute_nanos = Duration::minutes(1).num_nanoseconds().unwrap();
    RateLimits {
        send_order_request: MultiRateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 1200,
            interval_nanos: one_minute_nanos as u128,
        }]),
    }
}

// TODO ensure this is correct
fn request_weights() -> RequestWeights {
    RequestWeights {
        send_order_request: 1,
    }
}

fn signature_appender(
    api_key: String,
) -> SignatureAppender<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest> {
    Box::new(move |unsigned, signature| {
        let BinanceWebsocketUnsignedRequest {
            metadata,
            params: unsigned_params,
        } = unsigned;
        let signature = signature.map(|signature| BinanceSignature {
            apiKey: api_key.to_string(),
            signature,
        });
        let params = BinanceParams {
            params: unsigned_params,
            signature,
        };
        BinanceWebsocketRequest { metadata, params }
    })
}

fn id() -> String {
    Uuid::new_v4().to_string()
}
