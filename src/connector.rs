#[cfg(feature = "auto-resync")]
use crate::auto_resync_connector::AutoResyncConnector;
use crate::{
    clients::client::{HttpClient, WebsocketClient},
    clock::Clock,
    error::{EGError, EGResult},
    functions::BoxTryCreateOnce,
    rate_limit::{
        rate_limiter::RateLimiter, rate_limiter_state::RateLimiterState,
        rate_limiters::RateLimiters,
    },
    websocket_listener::WebsocketListener,
};
use exchange_types::{
    exchange::ETExchange,
    http::HttpResponse,
    new_types::{Milliseconds, UsageCount},
    rate_limited::{RateLimit, RateLimitRestriction},
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETResponse, ETWebsocketResponse},
    server_time::ServerTimeResponse,
    signer::Signer,
    urls::{Protocol, TradingMode, Urls},
    websocket_id::ETWebsocketId,
};
use futures_timer::Delay;
use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};
use strum::IntoEnumIterator;

#[cfg(feature = "iris")]
use {
    crate::clients::iris::IrisWebsocketClient,
    iris::{Config as IrisConfig, DisconnectedBehavior},
};

#[cfg(feature = "reqwest")]
use crate::clients::reqwest::ReqwestHttpClient;

pub struct Connector<Exchange, Client> {
    exchange: Exchange,
    rate_limiters: RateLimiters,
    clock: Clock,
    signer: Signer,
    client: Client,
    request_timeout: Duration,
    websocket_listener: Option<Arc<WebsocketListener>>,
}

impl Connector<(), ()> {
    pub fn try_new_http<Exchange, Client>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        client_creator: BoxTryCreateOnce<String, Client>,
        request_timeout: Duration,
    ) -> EGResult<Connector<Exchange, Client>>
    where
        Exchange: ETExchange,
        Client: HttpClient + Send + Sync,
    {
        let url = exchange
            .urls()
            .env_var_or_default(exchange.name(), Protocol::Http, trading_mode);
        let client = client_creator(url)?;
        let rate_limiters = Self::rate_limiters(exchange.default_capacity());
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            websocket_listener: None,
        })
    }
    #[cfg(feature = "reqwest")]
    pub fn try_new_http_reqwest<Exchange>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        request_timeout: Duration,
    ) -> EGResult<Connector<Exchange, ReqwestHttpClient>>
    where
        Exchange: ETExchange,
    {
        let client_creator = Box::new(move |url: String| Ok(ReqwestHttpClient::new(&url)));
        Self::try_new_http(
            trading_mode,
            exchange,
            signer,
            client_creator,
            request_timeout,
        )
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket<Exchange, Client>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        client_creator: BoxTryCreateOnce<(String, Arc<WebsocketListener>), Client>,
        request_timeout: Duration,
    ) -> EGResult<Connector<Exchange, Client>>
    where
        Exchange: ETExchange,
        Client: WebsocketClient,
    {
        let websocket_listener = Arc::new(WebsocketListener::new());
        let url =
            exchange
                .urls()
                .env_var_or_default(exchange.name(), Protocol::Websocket, trading_mode);
        let client = client_creator((url, websocket_listener.clone()))?;
        let rate_limiters = Self::rate_limiters(exchange.default_capacity());
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            websocket_listener: Some(websocket_listener),
        })
    }
    #[allow(clippy::type_complexity)]
    #[cfg(feature = "iris")]
    pub fn try_new_websocket_iris<Exchange>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        mut iris_config: IrisConfig,
        request_timeout: Duration,
    ) -> EGResult<Connector<Exchange, IrisWebsocketClient>>
    where
        Exchange: ETExchange,
    {
        iris_config = iris_config.with_disconnected_behavior(DisconnectedBehavior::DropAllQueued);
        let client_creator: BoxTryCreateOnce<
            (String, Arc<WebsocketListener>),
            IrisWebsocketClient,
        > = Box::new(move |(url, websocket_listener)| {
            Ok(IrisWebsocketClient::with_config(
                &url,
                iris_config,
                websocket_listener,
            ))
        });
        Self::try_new_websocket(
            trading_mode,
            exchange,
            signer,
            client_creator,
            request_timeout,
        )
    }
    fn rate_limiters(default_capacity: HashMap<RateLimit, UsageCount>) -> RateLimiters {
        let mut limiter_states = HashMap::new();
        for (rate_limit, capacity) in default_capacity {
            let RateLimit {
                restriction,
                interval_nanos,
            } = rate_limit;
            let states = limiter_states.entry(restriction).or_insert_with(Vec::new);
            let state = RateLimiterState::new(interval_nanos, capacity);
            states.push(state);
        }
        let limiters = limiter_states
            .iter()
            .map(|(restriction, states)| (*restriction, RateLimiter::new(states)))
            .collect::<HashMap<RateLimitRestriction, RateLimiter>>();
        RateLimiters::new(limiters)
    }
}

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
{
    #[cfg(feature = "auto-resync")]
    pub fn into_auto_resync(self) -> AutoResyncConnector<Exchange, Client>
    where
        Exchange: Send + Sync + 'static,
        Client: Send + Sync + 'static,
    {
        AutoResyncConnector::new(Arc::new(self))
    }
    pub fn duration_since_last_sync(&self) -> EGResult<Duration> {
        self.clock.duration_since_last_sync()
    }
    pub fn server_time_estimate(&self) -> EGResult<Milliseconds> {
        Ok(self.clock.server_time_estimate())
    }
    fn validate_rate_limits<Request>(
        &self,
        request: &Request,
    ) -> EGResult<Vec<(RateLimitRestriction, UsageCount)>>
    where
        Request: ETRequest,
    {
        let mut acquired = Vec::new();
        for restriction in RateLimitRestriction::iter() {
            let cost = request.rate_limit_usage(restriction);
            if cost > UsageCount::ZERO {
                if self.rate_limiters.did_acquire(restriction, cost)? {
                    acquired.push((restriction, cost));
                } else {
                    for (restriction, cost) in acquired {
                        let _ = self.rate_limiters.refund(restriction, cost);
                    }
                    return Err(EGError::RateLimited);
                }
            }
        }
        Ok(acquired)
    }
    fn refund(&self, costs: Vec<(RateLimitRestriction, UsageCount)>) {
        for (restriction, cost) in costs {
            let _ = self.rate_limiters.refund(restriction, cost);
        }
    }
    fn on_error<T>(
        &self,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<T> {
        if matches!(&error, EGError::RateLimited | EGError::NotSent(..)) {
            self.refund(costs);
        }
        Err(error)
    }
    fn set_rate_limits(&self, response: &impl ETResponse) -> EGResult<()> {
        if let Some(usage) = response.rate_limit_usage() {
            let _ = self.rate_limiters.set_usage(usage);
        }
        if let Some(retry_after_seconds) = response.retry_after() {
            let retry_after = Duration::from_secs(retry_after_seconds.0.max(0) as u64);
            let _ = self.rate_limiters.set_retry_after(retry_after);
            return Err(EGError::RateLimited);
        }
        Ok(())
    }
}

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: HttpClient,
{
    pub async fn sync_clock_http(&self) -> EGResult<()> {
        let server_time_request = self.exchange.server_time_request_http();
        let costs = self.validate_rate_limits(&server_time_request)?;
        let http_request = match server_time_request.try_into_http(&self.signer) {
            Ok(http_request) => http_request,
            Err(error) => {
                self.refund(costs);
                return Err(EGError::External(Box::new(error)));
            }
        };
        let start = Instant::now();
        let response = match self.client.send(http_request, self.request_timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let round_trip_time = start.elapsed();
        let response = self.validate_http_status(response)?;
        let response: Exchange::ServerTimeResponseHttp = Self::parse_http_response(response)?;
        self.set_rate_limits(&response)?;
        if let Some(server_time) = response.server_time() {
            self.clock.sync(server_time, round_trip_time)?;
        }
        Ok(())
    }
    pub async fn send_http<Response>(
        &self,
        mut request: impl ETHttpRequest<Exchange = Exchange, Response = Response>,
    ) -> EGResult<Response>
    where
        Response: ETHttpResponse,
    {
        request.set_timestamp(self.clock.server_time_estimate());
        let costs = self.validate_rate_limits(&request)?;
        let http_request = match request.try_into_http(&self.signer) {
            Ok(http_request) => http_request,
            Err(error) => {
                self.refund(costs);
                return Err(EGError::External(Box::new(error)));
            }
        };
        let response = match self.client.send(http_request, self.request_timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let response = self.validate_http_status(response)?;
        let response = Self::parse_http_response(response)?;
        self.set_rate_limits(&response)?;
        Ok(response)
    }
    fn parse_http_response<Response>(response: HttpResponse) -> EGResult<Response>
    where
        Response: ETHttpResponse,
    {
        let body = response.body.clone();
        Response::try_from_http(response).map_err(|source| EGError::HttpParseError { source, body })
    }
    fn validate_http_status(&self, response: HttpResponse) -> EGResult<HttpResponse> {
        if (200..300).contains(&response.status) {
            return Ok(response);
        }
        if let Some(retry_after_seconds) = response
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("Retry-After"))
            .and_then(|(_, value)| value.parse::<u64>().ok())
        {
            let _ = self
                .rate_limiters
                .set_retry_after(Duration::from_secs(retry_after_seconds));
            return Err(EGError::RateLimited);
        }
        Err(EGError::HttpError {
            status: response.status,
            body: response.body,
        })
    }
}

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> EGResult<()> {
        self.client.connect().await
    }
    pub fn is_connected(&self) -> EGResult<bool> {
        Ok(self.client.is_connected())
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        self.client.disconnect().await
    }
    pub async fn sync_clock_websocket(&self) -> EGResult<()> {
        let server_time_request = self.exchange.server_time_request_websocket();
        let costs = self.validate_rate_limits(&server_time_request)?;
        let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
        let (websocket_request, response_matcher) =
            match server_time_request.try_into_websocket(&self.signer, id) {
                Ok(request) => request,
                Err(_) => {
                    self.refund(costs);
                    return Err(EGError::BadResponse);
                }
            };
        let start = Instant::now();
        let response: Exchange::ServerTimeResponseWebsocket = self
            .send_wait(websocket_request, costs, response_matcher)
            .await?;
        let round_trip_time = start.elapsed();
        if let Some(server_time) = response.server_time() {
            self.clock.sync(server_time, round_trip_time)?;
        }
        Ok(())
    }
    pub async fn send_websocket<Response>(
        &self,
        mut request: impl ETWebsocketRequest<Exchange = Exchange, Response = Response>,
    ) -> EGResult<Response>
    where
        Response: ETWebsocketResponse,
    {
        request.set_timestamp(self.clock.server_time_estimate());
        let costs = self.validate_rate_limits(&request)?;
        let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
        let (websocket_request, response_matcher) =
            match request.try_into_websocket(&self.signer, id) {
                Ok(request) => request,
                Err(error) => {
                    self.refund(costs);
                    return Err(EGError::External(Box::new(error)));
                }
            };
        self.send_wait(websocket_request, costs, response_matcher)
            .await
    }
    async fn send_wait<Response>(
        &self,
        message: String,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
        response_matcher: Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>,
    ) -> EGResult<Response>
    where
        Response: ETWebsocketResponse,
    {
        let waiter = self
            .websocket_listener
            .as_ref()
            .expect("Error getting websocket listener")
            .waiter_for_filtered_response(response_matcher)?;
        let start = Instant::now();
        match self.client.send(message, self.request_timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let remaining = self.request_timeout.saturating_sub(start.elapsed());
        let mut waiter = Box::pin(waiter);
        let mut delay = Box::pin(Delay::new(remaining));
        let response_value = poll_fn(move |cx| match waiter.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
        .await?;
        let response = Response::try_from_websocket(response_value)
            .map_err(|e| EGError::External(Box::new(e)))?;
        self.set_rate_limits(&response)?;
        Ok(response)
    }
}

impl<Client, SyncRequest> std::fmt::Debug for Connector<Client, SyncRequest> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorImpl")
            .field("rate_limits", &self.rate_limiters)
            .field("clock", &self.clock)
            .field("signer", &"<signer>")
            .field("client", &"<client>")
            .field("auto_resync", &"<auto_resync>")
            .field("resync", &"<resync>")
            .finish()
    }
}
