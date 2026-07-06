use crate::{
    connector::{AuthenticateLeg, Connector},
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{SignatureAppender, TryConvertRequestTo, TryConvertResponseFrom, double_converter},
    listeners::{exchange_listener::ExchangeListener, listener::Listener},
    rate_limit::{
        multi_rate_limiter::MultiRateLimiter, rate_limit_config::RateLimitConfig,
        rate_limits::RateLimits, request_weights::RequestWeights,
    },
    sign::{
        convert_signer::ConvertSigner,
        encode::byte_encoding::ByteEncoding,
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
        message_signer::MessageSigner,
        signer::Signer,
    },
    transports::{
        http_transport::HttpMessageDto, transport::TransportTrait,
        transport_creator::TransportCreator, websocket_transport::WebsocketMessageDto,
    },
};
use chrono::{Duration, Utc};
use exchange_types::binance::{
    http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    logon::BinanceLogonParams,
    signed::{BinanceParams, BinanceSignature, BinanceUnsignedParams},
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

pub(crate) struct BinanceHttpConnectorCreator<TTransport, TRequest, TResponse>
where
    TTransport: TransportTrait<MessageDto = HttpMessageDto>,
{
    pub transport_creator: TransportCreator<TTransport, HttpMessageDto>,
    pub request_timeout: Duration,
    pub to_unsigned: TryConvertRequestTo<TRequest, BinanceHttpUnsignedRequest>,
    pub to_response: TryConvertResponseFrom<BinanceHttpResponse, TResponse>,
}
pub(crate) struct BinanceWebsocketConnectorCreator<TTransport, TRequest, TResponse>
where
    TTransport: TransportTrait<MessageDto = WebsocketMessageDto>,
{
    pub transport_creator: TransportCreator<TTransport, WebsocketMessageDto>,
    pub request_timeout: Duration,
    pub to_unsigned: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
    pub to_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
}

impl<TTransport, TRequest, TResponse>
    ConnectorCreatorTrait<
        TRequest,
        BinanceHttpUnsignedRequest,
        ApiKeyCredentials,
        BinanceHttpRequest,
        TTransport,
        BinanceHttpResponse,
        TResponse,
    > for BinanceHttpConnectorCreator<TTransport, TRequest, TResponse>
where
    TTransport: TransportTrait<MessageDto = HttpMessageDto> + 'static,
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    fn into_connector(
        self,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
            TTransport,
            BinanceHttpResponse,
            TResponse,
        >,
    > {
        let BinanceHttpConnectorCreator {
            transport_creator,
            request_timeout,
            to_unsigned,
            to_response,
        } = self;
        let response_converter = double_converter(Box::new(from_http_dto), to_response);
        let listener = Arc::new(ExchangeListener::new(response_converter, listener));
        let transport = transport_creator.create_transport(listener.clone())?;
        let authenticate_legs = vec![];
        Ok(Connector {
            request_to_unsigned: to_unsigned,
            null_signer: Box::new(ConvertSigner::new(http_converter)),
            message_out_to_dto: Box::new(to_http_dto),
            transport,
            listener,
            create_signer_from_credentials: create_http_signer_from_credentials,
            authenticate_legs,
            timeout: request_timeout,
            request_weights: request_weights(),
            rate_limits: rate_limits(),
        })
    }
}
impl<TTransport, TRequest, TResponse>
    ConnectorCreatorTrait<
        TRequest,
        BinanceWebsocketUnsignedRequest,
        ApiKeyCredentials,
        BinanceWebsocketRequest,
        TTransport,
        BinanceWebsocketResponse,
        TResponse,
    > for BinanceWebsocketConnectorCreator<TTransport, TRequest, TResponse>
where
    TTransport: TransportTrait<MessageDto = WebsocketMessageDto> + 'static,
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    fn into_connector(
        self,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            BinanceWebsocketRequest,
            TTransport,
            BinanceWebsocketResponse,
            TResponse,
        >,
    > {
        let BinanceWebsocketConnectorCreator {
            transport_creator,
            request_timeout,
            to_unsigned,
            to_response,
        } = self;
        let response_converter = double_converter(Box::new(from_websocket_dto), to_response);
        let listener = Arc::new(ExchangeListener::new(response_converter, listener));
        let transport = transport_creator.create_transport(listener.clone())?;
        let authenticate_legs = vec![];
        Ok(Connector {
            request_to_unsigned: to_unsigned,
            null_signer: Box::new(ConvertSigner::new(websocket_converter)),
            message_out_to_dto: Box::new(to_websocket_dto),
            transport,
            listener,
            create_signer_from_credentials: create_websocket_signer_from_credentials,
            authenticate_legs,
            timeout: request_timeout,
            request_weights: request_weights(),
            rate_limits: rate_limits(),
        })
    }
}

fn authenticate_websocket_leg() -> AuthenticateLeg<
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
fn create_signer_from_message(
    _message: BinanceWebsocketResponse,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    Ok(Box::new(ConvertSigner::new(websocket_converter)))
}

fn to_http_dto(message: &BinanceHttpRequest) -> EGResult<HttpMessageDto> {
    let body_json =
        serde_json::to_string(&message).map_err(|_e| EGError::Custom("".to_string()))?;
    Ok(HttpMessageDto {
        headers: HashMap::new(),
        body_json,
    })
}
fn from_http_dto(dto: HttpMessageDto) -> EGResult<BinanceHttpResponse> {
    let message: BinanceHttpResponse = serde_json::from_str(dto.body_json.as_str())
        .map_err(|_e| EGError::Custom("Failed to deserialize response".to_string()))?;
    Ok(message)
}

fn to_websocket_dto(message: &BinanceWebsocketRequest) -> EGResult<WebsocketMessageDto> {
    let body_json =
        serde_json::to_string(&message).map_err(|_e| EGError::Custom("".to_string()))?;
    Ok(WebsocketMessageDto { body_json })
}
fn from_websocket_dto(dto: WebsocketMessageDto) -> EGResult<BinanceWebsocketResponse> {
    let message: BinanceWebsocketResponse = serde_json::from_str(dto.body_json.as_str())
        .map_err(|_e| EGError::Custom("Failed to deserialize response".to_string()))?;
    Ok(message)
}

fn create_http_signer_from_credentials(
    credentials: ApiKeyCredentials,
) -> EGResult<Signer<BinanceHttpUnsignedRequest, BinanceHttpRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceHttpUnsignedRequest,
        BinanceHttpRequest,
    >::new(
        unsigned_params_to_bytes,
        data_signer(&secret)?,
        ByteEncoding::Base64,
        signature_appender_http(api_key),
    )))
}
fn create_websocket_signer_from_credentials(
    credentials: ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        websocket_unsigned_request_to_bytes,
        data_signer(&secret)?,
        ByteEncoding::Base64,
        signature_appender_websocket(api_key),
    )))
}
fn websocket_unsigned_request_to_bytes(
    request: &BinanceWebsocketUnsignedRequest,
) -> EGResult<Vec<u8>> {
    unsigned_params_to_bytes(&request.params)
}
fn unsigned_params_to_bytes(params: &BinanceUnsignedParams) -> EGResult<Vec<u8>> {
    Ok(serde_urlencoded::to_string(params).unwrap().into_bytes())
}
fn data_signer(secret: &SecretString) -> EGResult<DataSigner> {
    SigningAlgorithm::Ed25519
        .signer(secret)
        .map_err(|_e| EGError::Custom("Cannot create signer".to_string()))
}

fn http_converter(unsigned: BinanceHttpUnsignedRequest) -> EGResult<BinanceHttpRequest> {
    let request = BinanceHttpRequest {
        signature: None,
        params: unsigned,
    };
    Ok(request)
}
fn websocket_converter(
    unsigned: BinanceWebsocketUnsignedRequest,
) -> EGResult<BinanceWebsocketRequest> {
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

fn signature_appender_http(
    api_key: String,
) -> SignatureAppender<BinanceHttpUnsignedRequest, BinanceHttpRequest> {
    Box::new(move |unsigned, signature| {
        let signature = signature.map(|signature| BinanceSignature {
            apiKey: api_key.to_string(),
            signature,
        });
        BinanceHttpRequest {
            params: unsigned,
            signature,
        }
    })
}
fn signature_appender_websocket(
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
