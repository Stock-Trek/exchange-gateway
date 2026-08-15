use crate::{
    connector::{AuthenticateLeg, Connector},
    connector_creator::ConnectorCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    functions::{ArcCombineValues, ArcTryConvertValue, double_converter},
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
    urls::{ExchangeTransportType, ExchangeTransportUrls, ExchangeUrls, TradingMode},
};
use exchange_types::binance::{
    http::{BinanceHttpBody, BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    logon::BinanceLogonParams,
    signed::{BinanceSignature, BinanceSignedParams},
    websocket::{
        BinanceWebsocketBody, BinanceWebsocketMetadata, BinanceWebsocketMethodName,
        BinanceWebsocketRequest, BinanceWebsocketResponse, BinanceWebsocketUnsignedParams,
        BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct BinanceHttpConnectorCreator<TRequest, TResponse> {
    pub client_creator: CreateHttpClient<BinanceHttpBody>,
    pub to_unsigned: ArcTryConvertValue<TRequest, BinanceHttpUnsignedRequest>,
    pub to_response: ArcTryConvertValue<BinanceHttpResponse, TResponse>,
}

#[derive(Clone)]
pub(crate) struct BinanceWebsocketConnectorCreator<TRequest, TResponse> {
    pub client_creator: CreateWebsocketClient<BinanceWebsocketBody>,
    pub to_unsigned: ArcTryConvertValue<TRequest, BinanceWebsocketUnsignedRequest>,
    pub to_response: ArcTryConvertValue<BinanceWebsocketResponse, TResponse>,
}

impl<TFrom, TTo> std::fmt::Display for BinanceHttpConnectorCreator<TFrom, TTo> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceHttpConnectorCreator")
            .field("client_creator", &"<function>")
            .field("to_unsigned", &"<function>")
            .field("to_response", &"<function>")
            .finish()
    }
}

impl<TFrom, TTo> std::fmt::Display for BinanceWebsocketConnectorCreator<TFrom, TTo> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinanceWebsocketConnectorCreator")
            .field("client_creator", &"<function>")
            .field("to_unsigned", &"<function>")
            .field("to_response", &"<function>")
            .finish()
    }
}

impl<TRequest, TResponse>
    ConnectorCreatorTrait<
        TRequest,
        BinanceHttpUnsignedRequest,
        ApiKeyCredentials,
        BinanceHttpRequest,
        BinanceHttpBody,
        BinanceHttpResponse,
        TResponse,
    > for BinanceHttpConnectorCreator<TRequest, TResponse>
where
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    fn into_connector(
        self,
        trading_mode: TradingMode,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceHttpUnsignedRequest,
            ApiKeyCredentials,
            BinanceHttpRequest,
            BinanceHttpBody,
            BinanceHttpResponse,
            TResponse,
        >,
    > {
        let BinanceHttpConnectorCreator {
            client_creator,
            to_unsigned,
            to_response,
        } = self;
        let exchange_urls = exchange_urls();
        let url = exchange_urls.url(ExchangeTransportType::Http, trading_mode);
        let client = (client_creator)(&url);
        let transport_client = TransportClient::Http(client);
        let request_to_dto = Arc::new(to_http_dto);
        let dto_to_message_from =
            double_converter(Arc::new(filter_http_dto), Arc::new(from_http_dto));
        let message_from_to_response = to_response;
        let transport =
            Transport::<BinanceHttpRequest, BinanceHttpBody, BinanceHttpResponse, TResponse>::new(
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
            BinanceHttpBody,
            BinanceHttpResponse,
            TResponse,
        > {
            request_to_unsigned: to_unsigned,
            null_signer: ConvertSigner::new(http_converter),
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
        BinanceWebsocketBody,
        BinanceWebsocketResponse,
        TResponse,
    > for BinanceWebsocketConnectorCreator<TRequest, TResponse>
where
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    fn into_connector(
        self,
        trading_mode: TradingMode,
        listener: Listener<TResponse>,
    ) -> EGResult<
        Connector<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            ApiKeyCredentials,
            BinanceWebsocketRequest,
            BinanceWebsocketBody,
            BinanceWebsocketResponse,
            TResponse,
        >,
    > {
        let BinanceWebsocketConnectorCreator {
            client_creator,
            to_unsigned,
            to_response,
        } = self;
        let exchange_urls = exchange_urls();
        let url = exchange_urls.url(ExchangeTransportType::Websocket, trading_mode);
        let websocket_dto_to_message_from = Arc::new(from_websocket_dto);
        let converter =
            double_converter(websocket_dto_to_message_from.clone(), to_response.clone());
        let convert_listener = Arc::new(ConvertListener::new(converter, listener.clone()));
        let client = (client_creator)(&url, convert_listener);
        let transport_client = TransportClient::Websocket(client);
        let request_to_dto = Arc::new(to_websocket_dto);
        let dto_to_message_from = double_converter(
            Arc::new(filter_websocket_dto),
            websocket_dto_to_message_from,
        );
        let transport = Transport::<
            BinanceWebsocketRequest,
            BinanceWebsocketBody,
            BinanceWebsocketResponse,
            TResponse,
        >::new(
            transport_client,
            request_to_dto,
            dto_to_message_from,
            to_response,
            listener,
        );
        let authenticate_legs = vec![authenticate_websocket_leg()];
        Ok(Connector {
            request_to_unsigned: to_unsigned,
            null_signer: ConvertSigner::new(websocket_converter),
            transport,
            create_signer_from_credentials: create_websocket_signer_from_credentials,
            authenticate_legs,
            request_weights: request_weights(),
            rate_limits: rate_limits(),
        })
    }
}

fn exchange_urls() -> ExchangeUrls {
    ExchangeUrls::new(
        "BINANCE",
        ExchangeTransportUrls::new(
            "https://api.binance.com",
            "https://testnet.binance.vision/api",
        ),
        ExchangeTransportUrls::new(
            "wss://ws-fapi.binance.com/ws-fapi/v1",
            "wss://testnet.binancefuture.com/ws-fapi/v1",
        ),
    )
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
        filter_response: Arc::new(Ok),
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
        params: BinanceWebsocketUnsignedParams::Logon(params),
    }
}
fn create_signer_from_message(
    _message: BinanceWebsocketResponse,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    Ok(Box::new(ConvertSigner::new(websocket_converter)))
}

fn to_http_dto(message: BinanceHttpRequest) -> EGResult<TransportMessageDto<BinanceHttpBody>> {
    Ok(TransportMessageDto::Websocket(WebsocketMessageDto {
        body: BinanceHttpBody::Request(message),
    }))
}
fn from_http_dto(dto: HttpMessageDto<BinanceHttpBody>) -> EGResult<BinanceHttpResponse> {
    if let BinanceHttpBody::Response(response) = dto.body {
        Ok(response)
    } else {
        Err(EGError::ReceivedRequestInsteadOfResponse)
    }
}

fn to_websocket_dto(
    message: BinanceWebsocketRequest,
) -> EGResult<TransportMessageDto<BinanceWebsocketBody>> {
    Ok(TransportMessageDto::Websocket(WebsocketMessageDto {
        body: BinanceWebsocketBody::Request(message),
    }))
}
fn from_websocket_dto(
    dto: WebsocketMessageDto<BinanceWebsocketBody>,
) -> EGResult<BinanceWebsocketResponse> {
    if let BinanceWebsocketBody::Response(response) = dto.body {
        Ok(response)
    } else {
        Err(EGError::ReceivedRequestInsteadOfResponse)
    }
}

fn create_http_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceHttpUnsignedRequest, BinanceHttpRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceHttpUnsignedRequest,
        BinanceHttpRequest,
    >::new(
        http_unsigned_request_to_bytes,
        data_signer(secret)?,
        ByteEncoding::Base64,
        signature_appender_http(api_key.into()),
    )))
}
fn create_websocket_signer_from_credentials(
    credentials: &ApiKeyCredentials,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let ApiKeyCredentials { api_key, secret } = credentials;
    Ok(Box::new(MessageSigner::<
        BinanceWebsocketUnsignedRequest,
        BinanceWebsocketRequest,
    >::new(
        websocket_unsigned_request_params_to_bytes,
        data_signer(secret)?,
        ByteEncoding::Base64,
        signature_appender_websocket(api_key.into()),
    )))
}
fn http_unsigned_request_to_bytes(request: &BinanceHttpUnsignedRequest) -> EGResult<Vec<u8>> {
    Ok(serde_urlencoded::to_string(request)
        .map_err(|e| EGError::SerdeUrlencoded(format!("Failed to URL-encode params: {e}")))?
        .into_bytes())
}
fn websocket_unsigned_request_params_to_bytes(
    request: &BinanceWebsocketUnsignedRequest,
) -> EGResult<Vec<u8>> {
    Ok(serde_urlencoded::to_string(&request.params)
        .map_err(|e| EGError::SerdeUrlencoded(format!("Failed to URL-encode params: {e}")))?
        .into_bytes())
}
fn data_signer(secret: &SecretString) -> EGResult<DataSigner> {
    SigningAlgorithm::Ed25519.signer(secret)
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
    let params = BinanceSignedParams {
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
) -> ArcCombineValues<BinanceHttpUnsignedRequest, Option<String>, BinanceHttpRequest> {
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
) -> ArcCombineValues<BinanceWebsocketUnsignedRequest, Option<String>, BinanceWebsocketRequest> {
    Arc::new(move |unsigned, signature| {
        let BinanceWebsocketUnsignedRequest {
            metadata,
            params: unsigned_params,
        } = unsigned;
        let signature = signature.map(|signature| BinanceSignature {
            apiKey: api_key.to_string(),
            signature,
        });
        let params = BinanceSignedParams {
            params: unsigned_params,
            signature,
        };
        BinanceWebsocketRequest { metadata, params }
    })
}

fn id() -> String {
    Uuid::new_v4().to_string()
}
