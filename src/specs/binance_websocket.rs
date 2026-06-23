use crate::{
    cex::{
        cex_spec::{AuthenticateLeg, CexSpec, IncrementsLeg, RequestLeg},
        rate_limits_weights::RequestWeights,
    },
    connector::SpecCreatorTrait,
    credentials::api_key_credential::ApiKeyCredentials,
    error::{EGError, EGResult},
    exchange_spec::ExchangeSpec,
    functions::{SignatureAppender, TryConvertFromRequest, TryConvertToResponse, TryConvertValue},
    messenger::MessengerImpl,
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
use uuid::Uuid;

pub struct BinanceWebsocketSpecCreator<TTransport, TRequest, TResponse>
where
    TTransport: WebsocketTransportTrait,
{
    pub(crate) credentials: ApiKeyCredentials,
    pub(crate) transport: TTransport,
    pub(crate) use_session: bool,
    pub(crate) to_binance_params: TryConvertFromRequest<TRequest, BinanceUnsignedParams>,
    pub(crate) to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
}

impl<TTransport, TRequest, TResponse>
    SpecCreatorTrait<TRequest, BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest, TResponse>
    for BinanceWebsocketSpecCreator<TTransport, TRequest, TResponse>
where
    TTransport: WebsocketTransportTrait + 'static,
    TRequest: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    fn into_spec_signer(
        self,
    ) -> EGResult<(
        ExchangeSpec<TRequest, BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest, TResponse>,
        Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>,
    )> {
        let BinanceWebsocketSpecCreator {
            credentials,
            transport,
            to_binance_params,
            to_response,
            use_session,
        } = self;
        let messenger = Box::new(MessengerImpl::new(transport, dto, from_dto));
        let increments_leg = increments_leg(to_response);
        let authenticate_legs = if use_session {
            vec![authenticate_leg::<TResponse>()]
        } else {
            vec![]
        };
        let request_leg = request_leg(to_unsigned_message(to_binance_params), to_response);
        let spec = Box::new(CexSpec::<
            TRequest,
            BinanceWebsocketUnsignedRequest,
            BinanceWebsocketRequest,
            BinanceWebsocketResponse,
            TResponse,
        >::new(
            request_weights(),
            messenger,
            increments_leg,
            authenticate_legs,
            request_leg,
        ));
        let initial_signer = message_signer(credentials)?;
        Ok((spec, initial_signer))
    }
}

fn request_weights() -> RequestWeights {
    RequestWeights {
        send_order_request: 1,
    }
}

fn increments_leg<TResponse>(
    to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
) -> IncrementsLeg<BinanceWebsocketRequest, BinanceWebsocketResponse, TResponse> {
    let timeout = Duration::seconds(30);
    let message = increments_message();
    IncrementsLeg {
        message,
        timeout,
        to_response,
    }
}

fn authenticate_leg<TResponse>() -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let timeout = Duration::seconds(20);
    AuthenticateLeg {
        create_auth_message,
        create_signer_from,
        timeout,
    }
}

fn request_leg<TRequest, TResponse>(
    to_unsigned_message: TryConvertFromRequest<TRequest, BinanceWebsocketUnsignedRequest>,
    to_response: TryConvertToResponse<BinanceWebsocketResponse, TResponse>,
) -> RequestLeg<TRequest, BinanceWebsocketUnsignedRequest, BinanceWebsocketResponse, TResponse> {
    let timeout = Duration::seconds(10);
    RequestLeg::<TRequest, BinanceWebsocketUnsignedRequest, BinanceWebsocketResponse, TResponse> {
        timeout,
        to_unsigned_message,
        to_response,
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
    let timestamp = timestamp();
    let params = BinanceLogonParams { timestamp };
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id,
            method: BinanceWebsocketMethodName::Logon,
        },
        params: BinanceUnsignedParams::Logon(params),
    }
}

fn create_signer_from(
    _message_from: &BinanceWebsocketResponse,
) -> EGResult<Signer<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>> {
    let converter = converter();
    let signer = Box::new(ConvertSigner::new(converter));
    Ok(signer)
}

fn unsigned_message_bytes(message: &BinanceWebsocketUnsignedRequest) -> EGResult<Vec<u8>> {
    Ok(serde_urlencoded::to_string(message).unwrap().into_bytes())
}

fn to_unsigned_message<TRequest>(
    to_binance_params: TryConvertFromRequest<TRequest, BinanceUnsignedParams>,
) -> TryConvertFromRequest<TRequest, BinanceWebsocketUnsignedRequest>
where
    TRequest: 'static,
{
    Box::new(
        move |request| -> EGResult<BinanceWebsocketUnsignedRequest> {
            let id = id();
            let method = BinanceWebsocketMethodName::PlaceOrder;
            let params = to_binance_params(request)?;
            Ok(BinanceWebsocketUnsignedRequest {
                metadata: BinanceWebsocketMetadata { id, method },
                params,
            })
        },
    )
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

fn message_signer(
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

fn converter() -> TryConvertValue<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest> {
    |unsigned| {
        let BinanceWebsocketUnsignedRequest { metadata, params } = unsigned;
        let params =  BinanceParams {
            signature: None,
            params,
        };
        Ok(BinanceWebsocketRequest { metadata, params })
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
        let signature = if let Some(signature) = signature {
            Some(BinanceSignature {
                apiKey: api_key.to_string(),
                signature,
            })
        } else {
            None
        };
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

fn timestamp() -> i64 {
    Utc::now().timestamp()
}
