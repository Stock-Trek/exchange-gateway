use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    connector_impl::ConnectorImpl,
    credentials::api_key_credential::ApiKeyCredentials,
    error::EGResult,
    functions::{ArcCombineValues, ArcTryConvertValue},
    listeners::{
        convert_listener::ConvertListener, listener::ListenerTrait,
        websocket_listener::WebsocketListener,
    },
    rate_limit::{
        rate_limit_config::RateLimitConfig, rate_limiter::RateLimiter, rate_limits::RateLimits,
    },
    sign::{
        convert_signer::ConvertSigner,
        encode::byte_encoding::ByteEncoding,
        encrypt::{data_signer::DataSigner, signing_algorithm::SigningAlgorithm},
        message_signer::MessageSigner,
        signer::Signer,
    },
    transports::{
        http::{HttpClientTrait, HttpEndpoint, HttpTransport},
        transport::Transport,
        websocket::{WebsocketClientTrait, WebsocketTransport},
    },
    urls::{ExchangeTransportType, ExchangeTransportUrls, ExchangeUrls, TradingMode},
};
use exchange_types::binance::{
    http::{BinanceHttpRequest, BinanceHttpResponse, BinanceHttpUnsignedRequest},
    logon::BinanceLogonParams,
    signed::{BinanceSignature, BinanceSignedParams},
    spot::BinanceSpotOrderParams,
    websocket::{
        BinanceWebsocketMetadata, BinanceWebsocketMethodName, BinanceWebsocketRequest,
        BinanceWebsocketResponse, BinanceWebsocketUnsignedParams, BinanceWebsocketUnsignedRequest,
    },
};
use secrecy::SecretString;
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

type HttpCreateClient<TClient> = Box<dyn Fn(&str) -> TClient>;
type WebsocketCreateClient<TClient, WebsocketRes> =
    Box<dyn Fn(&str, Arc<dyn ListenerTrait<TMessage = WebsocketRes>>) -> TClient>;

/// Marker type indicating that a builder field has not been assigned yet.
pub struct Missing;

/// Marker type indicating that a builder field has been assigned.
pub struct Set;

pub struct HttpConnectorBuilder<
    TClient,
    ExternalReq,
    HttpReq,
    HttpRes,
    ExternalRes,
    ToUnsignedRequest,
    ToTransportRequest,
    ToBinanceResponse,
    ToExternalResponse,
    Listener,
> {
    trading_mode: TradingMode,
    create_client: HttpCreateClient<TClient>,
    to_unsigned_request: Option<ArcTryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>>,
    to_transport_request: Option<ArcTryConvertValue<BinanceHttpRequest, HttpReq>>,
    to_binance_response: Option<ArcTryConvertValue<HttpRes, BinanceHttpResponse>>,
    to_external_response: Option<ArcTryConvertValue<BinanceHttpResponse, ExternalRes>>,
    listener: Option<Arc<dyn ListenerTrait<TMessage = ExternalRes>>>,
    credentials: Option<ApiKeyCredentials>,
    _state: PhantomData<(
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    )>,
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        Missing,
        Missing,
        Missing,
        Missing,
        Missing,
    >
{
    pub fn new(create_client: impl Fn(&str) -> TClient + 'static) -> Self {
        Self {
            trading_mode: TradingMode::Real,
            create_client: Box::new(create_client),
            to_unsigned_request: None,
            to_transport_request: None,
            to_binance_response: None,
            to_external_response: None,
            listener: None,
            credentials: None,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, ToExternalResponse, Listener>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    pub fn trading_mode(mut self, trading_mode: TradingMode) -> Self {
        self.trading_mode = trading_mode;
        self
    }
    pub fn credentials(mut self, credentials: Option<ApiKeyCredentials>) -> Self {
        self.credentials = credentials;
        self
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToTransportRequest, ToBinanceResponse, ToExternalResponse, Listener>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        Missing,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    pub fn to_unsigned_request(
        self,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceHttpUnsignedRequest>,
    ) -> HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        Set,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    > {
        HttpConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: Some(to_unsigned_request),
            to_transport_request: self.to_transport_request,
            to_binance_response: self.to_binance_response,
            to_external_response: self.to_external_response,
            listener: self.listener,
            credentials: self.credentials,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToUnsignedRequest, ToBinanceResponse, ToExternalResponse, Listener>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        Missing,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    pub fn to_transport_request(
        self,
        to_transport_request: ArcTryConvertValue<BinanceHttpRequest, HttpReq>,
    ) -> HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        Set,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    > {
        HttpConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: Some(to_transport_request),
            to_binance_response: self.to_binance_response,
            to_external_response: self.to_external_response,
            listener: self.listener,
            credentials: self.credentials,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToExternalResponse, Listener>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        Missing,
        ToExternalResponse,
        Listener,
    >
{
    pub fn to_binance_response(
        self,
        to_binance_response: ArcTryConvertValue<HttpRes, BinanceHttpResponse>,
    ) -> HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        Set,
        ToExternalResponse,
        Listener,
    > {
        HttpConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: self.to_transport_request,
            to_binance_response: Some(to_binance_response),
            to_external_response: self.to_external_response,
            listener: self.listener,
            credentials: self.credentials,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, Listener>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        Missing,
        Listener,
    >
{
    pub fn to_external_response(
        self,
        to_external_response: ArcTryConvertValue<BinanceHttpResponse, ExternalRes>,
    ) -> HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        Set,
        Listener,
    > {
        HttpConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: self.to_transport_request,
            to_binance_response: self.to_binance_response,
            to_external_response: Some(to_external_response),
            listener: self.listener,
            credentials: self.credentials,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, ToExternalResponse>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Missing,
    >
{
    pub fn listener(
        self,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    ) -> HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Set,
    > {
        HttpConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: self.to_transport_request,
            to_binance_response: self.to_binance_response,
            to_external_response: self.to_external_response,
            listener: Some(listener),
            credentials: self.credentials,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes>
    HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        Set,
        Set,
        Set,
        Set,
        Set,
    >
{
    pub fn build(self) -> impl Connector<ExternalReq, ExternalRes>
    where
        TClient: HttpClientTrait<TransportReq = HttpReq, TransportRes = HttpRes> + 'static,
        ExternalReq: Send,
        HttpReq: Send,
        HttpRes: Send + 'static,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        let exchange_urls = exchange_urls();
        let url = exchange_urls.url(ExchangeTransportType::Http, self.trading_mode);
        let client = Arc::new((self.create_client)(&url));
        let to_external_response = self
            .to_external_response
            .expect("to_external_response is required to build the binance http connector");
        let listener = self
            .listener
            .expect("listener is required to build the binance http connector");
        let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceHttpResponse>> =
            Arc::new(ConvertListener::new(to_external_response, listener));
        let to_transport_request = self
            .to_transport_request
            .expect("to_transport_request is required to build the binance http connector");
        let to_binance_response = self
            .to_binance_response
            .expect("to_binance_response is required to build the binance http connector");
        let http_transport = HttpTransport::new(
            client,
            to_transport_request,
            to_binance_response,
            response_listener,
            request_to_http_endpoint,
            http_endpoints(),
        );
        let to_unsigned_request = self
            .to_unsigned_request
            .expect("to_unsigned_request is required to build the binance http connector");
        let signer = Arc::new(Mutex::new(self.credentials.as_ref().map(|credentials| {
            create_http_signer_from_credentials(credentials)
                .expect("Failed to create signer from credentials")
        })));
        ConnectorImpl {
            rate_limits: rate_limits(),
            to_weight: http_request_weight,
            to_unsigned_request,
            transport: Transport::Http(http_transport),
            null_signer: null_http_signer(),
            credentials: self.credentials,
            create_signer: create_http_signer_from_credentials,
            authenticate_legs: vec![],
            signer,
        }
    }
}

impl<TClient, ExternalReq, HttpReq, HttpRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, ToExternalResponse, Listener> std::fmt::Debug
    for HttpConnectorBuilder<
        TClient,
        ExternalReq,
        HttpReq,
        HttpRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpConnectorBuilder")
            .field("trading_mode", &self.trading_mode)
            .field("create_client", &"<function>")
            .field("to_unsigned_request", &"<function>")
            .field("to_transport_request", &"<function>")
            .field("to_binance_response", &"<function>")
            .field("to_external_response", &"<function>")
            .field("listener", &"<Listener>")
            .field("credentials", &"<redacted>")
            .finish()
    }
}

pub struct WebsocketConnectorBuilder<
    TClient,
    ExternalReq,
    WebsocketReq,
    WebsocketRes,
    ExternalRes,
    ToUnsignedRequest,
    ToTransportRequest,
    ToBinanceResponse,
    ToExternalResponse,
    Listener,
> {
    trading_mode: TradingMode,
    create_client: WebsocketCreateClient<TClient, WebsocketRes>,
    to_unsigned_request: Option<ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>>,
    to_transport_request: Option<ArcTryConvertValue<BinanceWebsocketRequest, WebsocketReq>>,
    to_binance_response: Option<ArcTryConvertValue<WebsocketRes, BinanceWebsocketResponse>>,
    to_external_response: Option<ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>>,
    listener: Option<Arc<dyn ListenerTrait<TMessage = ExternalRes>>>,
    credentials: Option<ApiKeyCredentials>,
    use_session: bool,
    _state: PhantomData<(
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    )>,
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        Missing,
        Missing,
        Missing,
        Missing,
        Missing,
    >
{
    pub fn new(
        create_client: impl Fn(&str, Arc<dyn ListenerTrait<TMessage = WebsocketRes>>) -> TClient
        + 'static,
    ) -> Self {
        Self {
            trading_mode: TradingMode::Real,
            create_client: Box::new(create_client),
            to_unsigned_request: None,
            to_transport_request: None,
            to_binance_response: None,
            to_external_response: None,
            listener: None,
            credentials: None,
            use_session: false,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, ToExternalResponse, Listener>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    pub fn trading_mode(mut self, trading_mode: TradingMode) -> Self {
        self.trading_mode = trading_mode;
        self
    }
    pub fn credentials(mut self, credentials: Option<ApiKeyCredentials>) -> Self {
        self.credentials = credentials;
        self
    }
    pub fn use_session(mut self, use_session: bool) -> Self {
        self.use_session = use_session;
        self
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToTransportRequest, ToBinanceResponse, ToExternalResponse, Listener>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        Missing,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    pub fn to_unsigned_request(
        self,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, BinanceWebsocketUnsignedRequest>,
    ) -> WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        Set,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    > {
        WebsocketConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: Some(to_unsigned_request),
            to_transport_request: self.to_transport_request,
            to_binance_response: self.to_binance_response,
            to_external_response: self.to_external_response,
            listener: self.listener,
            credentials: self.credentials,
            use_session: self.use_session,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToUnsignedRequest, ToBinanceResponse, ToExternalResponse, Listener>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        Missing,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    pub fn to_transport_request(
        self,
        to_transport_request: ArcTryConvertValue<BinanceWebsocketRequest, WebsocketReq>,
    ) -> WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        Set,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    > {
        WebsocketConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: Some(to_transport_request),
            to_binance_response: self.to_binance_response,
            to_external_response: self.to_external_response,
            listener: self.listener,
            credentials: self.credentials,
            use_session: self.use_session,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToExternalResponse, Listener>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        Missing,
        ToExternalResponse,
        Listener,
    >
{
    pub fn to_binance_response(
        self,
        to_binance_response: ArcTryConvertValue<WebsocketRes, BinanceWebsocketResponse>,
    ) -> WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        Set,
        ToExternalResponse,
        Listener,
    > {
        WebsocketConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: self.to_transport_request,
            to_binance_response: Some(to_binance_response),
            to_external_response: self.to_external_response,
            listener: self.listener,
            credentials: self.credentials,
            use_session: self.use_session,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, Listener>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        Missing,
        Listener,
    >
{
    pub fn to_external_response(
        self,
        to_external_response: ArcTryConvertValue<BinanceWebsocketResponse, ExternalRes>,
    ) -> WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        Set,
        Listener,
    > {
        WebsocketConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: self.to_transport_request,
            to_binance_response: self.to_binance_response,
            to_external_response: Some(to_external_response),
            listener: self.listener,
            credentials: self.credentials,
            use_session: self.use_session,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, ToExternalResponse>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Missing,
    >
{
    pub fn listener(
        self,
        listener: Arc<dyn ListenerTrait<TMessage = ExternalRes>>,
    ) -> WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Set,
    > {
        WebsocketConnectorBuilder {
            trading_mode: self.trading_mode,
            create_client: self.create_client,
            to_unsigned_request: self.to_unsigned_request,
            to_transport_request: self.to_transport_request,
            to_binance_response: self.to_binance_response,
            to_external_response: self.to_external_response,
            listener: Some(listener),
            credentials: self.credentials,
            use_session: self.use_session,
            _state: PhantomData,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes>
    WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        Set,
        Set,
        Set,
        Set,
        Set,
    >
{
    pub fn build(self) -> impl Connector<ExternalReq, ExternalRes>
    where
        TClient: WebsocketClientTrait<TransportReq = WebsocketReq, TransportRes = WebsocketRes>
            + 'static,
        ExternalReq: Send,
        WebsocketReq: Send,
        WebsocketRes: Send + 'static,
        ExternalRes: Clone + Send + Sync + 'static,
    {
        let exchange_urls = exchange_urls();
        let url = exchange_urls.url(ExchangeTransportType::Websocket, self.trading_mode);
        let to_external_response = self
            .to_external_response
            .expect("to_external_response is required to build the binance websocket connector");
        let listener = self
            .listener
            .expect("listener is required to build the binance websocket connector");
        let response_listener: Arc<dyn ListenerTrait<TMessage = BinanceWebsocketResponse>> =
            Arc::new(ConvertListener::new(to_external_response, listener));
        let to_binance_response = self
            .to_binance_response
            .expect("to_binance_response is required to build the binance websocket connector");
        let websocket_listener = Arc::new(WebsocketListener::new(
            to_binance_response.clone(),
            response_listener,
        ));
        let client = Arc::new((self.create_client)(&url, websocket_listener.clone()));
        let to_transport_request = self
            .to_transport_request
            .expect("to_transport_request is required to build the binance websocket connector");
        let websocket_transport = WebsocketTransport::new(
            client,
            to_transport_request,
            to_binance_response,
            websocket_listener,
        );
        let (authenticate_legs, signer) = if self.use_session {
            (
                vec![authenticate_websocket_leg()],
                Arc::new(Mutex::new(None)),
            )
        } else {
            (
                vec![],
                Arc::new(Mutex::new(self.credentials.as_ref().map(|credentials| {
                    create_websocket_signer_from_credentials(credentials)
                        .expect("Failed to create signer from credentials")
                }))),
            )
        };
        let to_unsigned_request = self
            .to_unsigned_request
            .expect("to_unsigned_request is required to build the binance websocket connector");
        ConnectorImpl {
            rate_limits: rate_limits(),
            to_weight: websocket_request_weight,
            to_unsigned_request,
            transport: Transport::Websocket(websocket_transport),
            null_signer: null_websocket_signer(),
            credentials: self.credentials,
            create_signer: create_websocket_signer_from_credentials,
            authenticate_legs,
            signer,
        }
    }
}

impl<TClient, ExternalReq, WebsocketReq, WebsocketRes, ExternalRes, ToUnsignedRequest, ToTransportRequest, ToBinanceResponse, ToExternalResponse, Listener> std::fmt::Debug
    for WebsocketConnectorBuilder<
        TClient,
        ExternalReq,
        WebsocketReq,
        WebsocketRes,
        ExternalRes,
        ToUnsignedRequest,
        ToTransportRequest,
        ToBinanceResponse,
        ToExternalResponse,
        Listener,
    >
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketConnectorBuilder")
            .field("trading_mode", &self.trading_mode)
            .field("create_client", &"<function>")
            .field("to_unsigned_request", &"<function>")
            .field("to_transport_request", &"<function>")
            .field("to_binance_response", &"<function>")
            .field("to_external_response", &"<function>")
            .field("listener", &"<Listener>")
            .field("credentials", &"<redacted>")
            .field("use_session", &self.use_session)
            .finish()
    }
}

fn exchange_urls() -> ExchangeUrls {
    ExchangeUrls::new(
        "BINANCE",
        ExchangeTransportUrls::new(
            "https://api.binance.com/api/v3",
            "https://testnet.binance.vision/api/v3",
        ),
        ExchangeTransportUrls::new(
            "wss://ws-api.binance.com:443/ws-api/v3",
            "wss://ws-api.testnet.binance.vision:443/ws-api/v3",
        ),
    )
}
fn request_to_http_endpoint(request: &BinanceHttpRequest) -> HttpEndpoint {
    match request.params {
        BinanceHttpUnsignedRequest::AssetLimits => HttpEndpoint::AssetLimits,
        BinanceHttpUnsignedRequest::ExchangeInfo(..) => HttpEndpoint::ExchangeInfo,
        BinanceHttpUnsignedRequest::SpotOrderRequest(..) => HttpEndpoint::PlaceOrder,
    }
}
fn http_endpoints() -> HashMap<HttpEndpoint, String> {
    let mut endpoints = HashMap::new();
    endpoints.insert(HttpEndpoint::AssetLimits, "myFilters".into());
    endpoints.insert(HttpEndpoint::ExchangeInfo, "exchangeInfo".into());
    endpoints.insert(HttpEndpoint::PlaceOrder, "order".into());
    endpoints
}

fn authenticate_websocket_leg() -> AuthenticateLeg<
    BinanceWebsocketUnsignedRequest,
    BinanceWebsocketRequest,
    BinanceWebsocketResponse,
> {
    let timeout = Duration::from_secs(20);
    let id = Arc::new(id());
    let create_auth_message = {
        let id = id.clone();
        Arc::new(move || create_auth_message(&id))
    };
    let filter = {
        let id = id.clone();
        Arc::new(move |response: &BinanceWebsocketResponse| response.id == *id)
    };
    AuthenticateLeg {
        create_auth_message,
        create_signer: create_signer_from_message,
        filter,
        timeout,
    }
}
fn create_auth_message(id: &str) -> BinanceWebsocketUnsignedRequest {
    let timestamp: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Negative time since epoch")
        .as_millis()
        .try_into()
        .expect("Epoch too large");
    let params = BinanceLogonParams { timestamp };
    BinanceWebsocketUnsignedRequest {
        metadata: BinanceWebsocketMetadata {
            id: id.to_string(),
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

fn null_http_signer() -> ConvertSigner<BinanceHttpUnsignedRequest, BinanceHttpRequest> {
    ConvertSigner::new(|unsigned| {
        Ok(BinanceHttpRequest {
            params: unsigned,
            signature: None,
        })
    })
}
fn null_websocket_signer() -> ConvertSigner<BinanceWebsocketUnsignedRequest, BinanceWebsocketRequest>
{
    ConvertSigner::new(|unsigned| {
        let BinanceWebsocketUnsignedRequest { metadata, params } = unsigned;
        Ok(BinanceWebsocketRequest {
            metadata,
            params: BinanceSignedParams {
                params,
                signature: None,
            },
        })
    })
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
        ByteEncoding::HexLower,
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
        ByteEncoding::HexLower,
        signature_appender_websocket(api_key.into()),
    )))
}
fn http_unsigned_request_to_bytes(
    request: &BinanceHttpUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match request {
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        _ => None,
    })
}
fn websocket_unsigned_request_params_to_bytes(
    request: &BinanceWebsocketUnsignedRequest,
) -> EGResult<Option<Vec<u8>>> {
    Ok(match &request.params {
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::Logon(params) => {
            Some(params.query_params(true).into_bytes())
        }
        BinanceWebsocketUnsignedParams::ExchangeInfo(_) => None,
    })
}
fn data_signer(secret: &SecretString) -> EGResult<DataSigner> {
    SigningAlgorithm::HmacSha256.signer(secret)
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

fn rate_limits() -> RateLimits {
    RateLimits {
        request: RateLimiter::new(vec![RateLimitConfig {
            capacity_per_interval: 1200,
            interval_nanos: Duration::from_mins(1).as_nanos(),
        }]),
    }
}

fn http_request_weight(request: &BinanceHttpUnsignedRequest) -> u32 {
    match request {
        BinanceHttpUnsignedRequest::ExchangeInfo(_) | BinanceHttpUnsignedRequest::AssetLimits => 1,
        BinanceHttpUnsignedRequest::SpotOrderRequest(params) => order_weight(params),
    }
}
fn websocket_request_weight(request: &BinanceWebsocketUnsignedRequest) -> u32 {
    match &request.params {
        BinanceWebsocketUnsignedParams::ExchangeInfo(_)
        | BinanceWebsocketUnsignedParams::Logon(_) => 1,
        BinanceWebsocketUnsignedParams::SpotOrderRequest(params) => order_weight(params),
    }
}
fn order_weight(params: &BinanceSpotOrderParams) -> u32 {
    if params.icebergQty.is_some()
        || params.trailingDelta.is_some()
        || params.pegPriceType.is_some()
        || params.pegOffsetValue.is_some()
    {
        2
    } else {
        1
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
