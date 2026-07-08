use crate::{
    connector::{AuthenticateLeg, Connector},
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{SignatureAppender, TryConvertRequestTo, TryConvertResponseFrom, double_converter},
    listeners::{convert_listener::ConvertListener, listener::Listener},
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
        http::{CreateHttpClient, HttpMessageDto},
        transport::{
            Transport, TransportClient, TransportMessageDto, filter_http_dto, filter_websocket_dto,
        },
        websocket::{CreateWebsocketClient, WebsocketMessageDto},
    },
};
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
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
pub(crate) struct BinanceHttpConnectorCreator<TRequest, TResponse> {
    pub client_creator: CreateHttpClient,
    pub to_unsigned: TryConvertRequestTo<TRequest, BinanceHttpUnsignedRequest>,
    pub to_response: TryConvertResponseFrom<BinanceHttpResponse, TResponse>,
}
pub(crate) struct BinanceWebsocketConnectorCreator<TRequest, TResponse> {
    pub client_creator: CreateWebsocketClient,
    pub to_unsigned: TryConvertRequestTo<TRequest, BinanceWebsocketUnsignedRequest>,
    pub to_response: TryConvertResponseFrom<BinanceWebsocketResponse, TResponse>,
}

impl<TRequest, TResponse>
    ConnectorCreatorTrait<
        TRequest,
        BinanceHttpUnsignedRequest,
        ApiKeyCredentials,
        BinanceHttpRequest,
        BinanceHttpResponse,
        TResponse,
    > for BinanceHttpConnectorCreator<TRequest, TResponse>
where
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
            BinanceHttpResponse,
            TResponse,
        >,
    > {
        let BinanceHttpConnectorCreator {
            client_creator,
            to_unsigned,
            to_response,
        } = self;
        let client = (client_creator)();
        let transport_client = TransportClient::Http(client);
        let request_to_dto = Arc::new(to_http_dto);
        let dto_to_message_from =
            double_converter(Arc::new(filter_http_dto), Arc::new(from_http_dto));
        let message_from_to_response = to_response;
        let transport = Transport::<BinanceHttpRequest, BinanceHttpResponse, TResponse>::new(
            transport_client,
            request_to_dto,
            dto_to_message_from,
            message_from_to_response,
            listener,
        );
        Ok(Connector::<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
            BinanceHttpResponse,
            TResponse,
        > {
            request_to_unsigned: to_unsigned,
            null_signer: Box::new(ConvertSigner::new(http_converter)),
            message_out_to_dto: Arc::new(to_http_dto),
            transport,
            create_signer_from_credentials: create_http_signer_from_credentials,
            authenticate_legs: Vec::new(),
            request_weights: request_weights(),
            rate_limits: rate_limits(),
        })
    }
}
impl<TRequest, TResponse>
    ConnectorCreatorTrait<
        TRequest,
        BinanceWebsocketUnsignedRequest,
        ApiKeyCredentials,
        BinanceWebsocketRequest,
        BinanceWebsocketResponse,
        TResponse,
    > for BinanceWebsocketConnectorCreator<TRequest, TResponse>
where
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
            BinanceWebsocketResponse,
            TResponse,
        >,
    > {
        let BinanceWebsocketConnectorCreator {
            client_creator,
            to_unsigned,
            to_response,
        } = self;
        let converter = double_converter(Arc::new(from_websocket_dto), to_response.clone());
        let convert_listener = Arc::new(ConvertListener::new(converter, listener.clone()));
        let client = (client_creator)(convert_listener);
        let transport_client = TransportClient::Websocket(client);
        let request_to_dto = Arc::new(to_websocket_dto);
        let dto_to_message_from =
            double_converter(Arc::new(filter_websocket_dto), Arc::new(from_websocket_dto));
        let transport =
            Transport::<BinanceWebsocketRequest, BinanceWebsocketResponse, TResponse>::new(
                transport_client,
                request_to_dto,
                dto_to_message_from,
                to_response,
                listener,
            );
        let authenticate_legs = vec![authenticate_websocket_leg()];
        Ok(Connector {
            request_to_unsigned: to_unsigned,
            null_signer: Box::new(ConvertSigner::new(websocket_converter)),
            message_out_to_dto: Arc::new(to_websocket_dto),
            transport,
            create_signer_from_credentials: create_websocket_signer_from_credentials,
            authenticate_legs,
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
    let timeout = Duration::from_secs(20);
    AuthenticateLeg {
        create_auth_message,
        create_signer_from: create_signer_from_message,
        filter_response: Arc::new(|m| Ok(m)),
        timeout,
    }
}
fn create_auth_message() -> BinanceWebsocketUnsignedRequest {
    let id = id();
    let timestamp: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Negative time since epoch")
        .as_millis()
        .try_into()
        .expect("Epoch too large");
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

fn to_http_dto(message: &BinanceHttpRequest) -> EGResult<TransportMessageDto> {
    let body_json =
        serde_json::to_string(&message).map_err(|_e| EGError::Custom("".to_string()))?;
    Ok(TransportMessageDto::Http(HttpMessageDto {
        headers: HashMap::new(),
        body_json,
    }))
}
fn from_http_dto(dto: HttpMessageDto) -> EGResult<BinanceHttpResponse> {
    let message: BinanceHttpResponse = serde_json::from_str(dto.body_json.as_str())
        .map_err(|_e| EGError::Custom("Failed to deserialize response".to_string()))?;
    Ok(message)
}

fn to_websocket_dto(message: &BinanceWebsocketRequest) -> EGResult<TransportMessageDto> {
    let body_json =
        serde_json::to_string(&message).map_err(|_e| EGError::Custom("".to_string()))?;
    Ok(TransportMessageDto::Websocket(WebsocketMessageDto {
        body_json,
    }))
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
    RateLimits {
        send_order_request: MultiRateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 1200,
            interval_nanos: Duration::from_mins(1).as_nanos(),
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
    Arc::new(move |unsigned, signature| {
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
    Arc::new(move |unsigned, signature| {
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
