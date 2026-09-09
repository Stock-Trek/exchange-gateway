use crate::{
    auto_resync::{AutoResync, Resync, ResyncFn, ResyncFuture},
    clients::{
        client::{HttpClient, WebsocketClient},
        iris::IrisWebsocketClient,
        reqwest::ReqwestHttpClient,
    },
    clock::Clock,
    error::{EGError, EGResult},
    functions::BoxTryCreateOnce,
    rate_limit::{
        rate_limiter::RateLimiter, rate_limiter_state::RateLimiterState,
        rate_limiters::RateLimiters,
    },
    websocket_listener::WebsocketListener,
};
use async_trait::async_trait;
use exchange_types::{
    exchange::ETExchange,
    http::HttpResponse,
    new_types::UsageCount,
    rate_limited::{RateLimit, RateLimitRestriction},
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETResponse, ETWebsocketResponse},
    server_time::{ServerTimeHttpRequest, ServerTimeResponse, ServerTimeWebsocketRequest},
    signer::Signer,
    urls::{Protocol, TradingMode, Urls},
    websocket_id::ETWebsocketId,
};
use futures_timer::Delay;
use iris::Config as IrisConfig;
use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, Instant},
};
use strum::IntoEnumIterator;

#[cfg(feature = "iris")]
use iris::DisconnectedBehavior;

pub struct Connector<Client> {
    rate_limiters: RateLimiters,
    clock: Clock,
    signer: Arc<Signer>,
    client: Arc<Client>,
    auto_resync: AutoResync,
    resync: Arc<Mutex<Option<ResyncFn>>>,
}

#[async_trait]
impl<Client, SyncRequest, SyncResponse> Resync<SyncRequest, SyncResponse> for Connector<Client>
where
    Client: HttpClient + Send + Sync,
    SyncRequest: ServerTimeHttpRequest<SyncResponse> + Send + 'static,
    SyncResponse: ETHttpResponse + ServerTimeResponse + Send,
{
    async fn resync(&self, request: SyncRequest, timeout: Duration) -> EGResult<()> {
        self.sync_clock(request, timeout).await
    }
}

#[async_trait]
impl<Client, SyncRequest, SyncResponse> Resync<SyncRequest, SyncResponse>
    for Connector<(Client, Arc<WebsocketListener>)>
where
    Client: WebsocketClient,
    SyncRequest: ServerTimeWebsocketRequest<SyncResponse> + Send + 'static,
    SyncResponse: ETWebsocketResponse + ServerTimeResponse + Send,
{
    async fn resync(&self, request: SyncRequest, timeout: Duration) -> EGResult<()> {
        self.sync_clock(request, timeout).await
    }
}

impl Connector<()> {
    pub fn try_new_http<Client>(
        trading_mode: TradingMode,
        exchange: impl ETExchange,
        signer: Signer,
        client_creator: BoxTryCreateOnce<String, Client>,
    ) -> EGResult<Connector<Client>>
    where
        Client: HttpClient,
    {
        let url = exchange
            .urls()
            .env_var_or_default(exchange.name(), Protocol::Http, trading_mode);
        let client = Arc::new(client_creator(url)?);
        Ok(Connector::<Client> {
            rate_limiters: Self::rate_limiters(exchange.default_capacity()),
            clock: Clock::default(),
            signer: Arc::new(signer),
            client,
            auto_resync: AutoResync::default(),
            resync: Arc::new(Mutex::new(None)),
        })
    }
    #[cfg(feature = "reqwest")]
    pub fn try_new_http_reqwest(
        trading_mode: TradingMode,
        exchange: impl ETExchange,
        signer: Signer,
    ) -> EGResult<Connector<ReqwestHttpClient>> {
        let client_creator = Box::new(move |url: String| Ok(ReqwestHttpClient::new(&url)));
        Self::try_new_http(trading_mode, exchange, signer, client_creator)
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket<Client>(
        trading_mode: TradingMode,
        exchange: impl ETExchange,
        signer: Signer,
        client_creator: BoxTryCreateOnce<(String, Arc<WebsocketListener>), Client>,
    ) -> EGResult<Connector<(Client, Arc<WebsocketListener>)>>
    where
        Client: WebsocketClient,
    {
        let websocket_listener = Arc::new(WebsocketListener::new());
        let url =
            exchange
                .urls()
                .env_var_or_default(exchange.name(), Protocol::Websocket, trading_mode);
        let client = client_creator((url, websocket_listener.clone()))?;
        Ok(Connector::<(Client, Arc<WebsocketListener>)> {
            rate_limiters: Self::rate_limiters(exchange.default_capacity()),
            clock: Clock::new(),
            signer: Arc::new(signer),
            client: Arc::new((client, websocket_listener)),
            auto_resync: AutoResync::default(),
            resync: Arc::new(Mutex::new(None)),
        })
    }
    #[allow(clippy::type_complexity)]
    #[cfg(feature = "iris")]
    pub fn try_new_websocket_iris(
        trading_mode: TradingMode,
        exchange: impl ETExchange,
        signer: Signer,
        mut iris_config: IrisConfig,
    ) -> EGResult<Connector<(IrisWebsocketClient, Arc<WebsocketListener>)>> {
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
        Self::try_new_websocket(trading_mode, exchange, signer, client_creator)
    }
}

impl<Client> Connector<Client> {
    pub fn set_auto_resync_clock<SyncRequest, SyncResponse>(
        &self,
        sync_request: SyncRequest,
        timeout: Duration,
    ) -> EGResult<()>
    where
        Self: Resync<SyncRequest, SyncResponse>,
        Client: Send + Sync + 'static,
        SyncRequest: Clone + Send + Sync + 'static,
    {
        let connector = self.clone();
        let resync: ResyncFn = Arc::new(move || {
            let connector = connector.clone();
            let sync_request = sync_request.clone();
            let future: ResyncFuture =
                Box::pin(async move { Resync::resync(&connector, sync_request, timeout).await });
            future
        });
        *self.resync.lock().map_err(|_| EGError::MutexPoisoned)? = Some(resync);
        Ok(())
    }
    pub fn auto_resync_clock(&self, duration: Option<Duration>) -> EGResult<()> {
        match duration {
            None => self.auto_resync.stop(),
            Some(duration) => {
                let resync = self
                    .resync
                    .lock()
                    .map_err(|_| EGError::MutexPoisoned)?
                    .clone()
                    .ok_or(EGError::AutoResyncClockNotConfigured)?;
                self.auto_resync.start_or_update(duration, move || resync())
            }
        }
    }
}

impl<Client> Connector<Client> {
    pub fn server_time_millis(&self) -> EGResult<i64> {
        Ok(self.clock.now_millis())
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
    fn on_error<T>(
        &self,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<T> {
        if matches!(&error, EGError::RateLimited | EGError::NotSent(..)) {
            for (restriction, cost) in costs {
                let _ = self.rate_limiters.refund(restriction, cost);
            }
        }
        Err(error)
    }
    fn set_rate_limits(&self, response: &impl ETResponse) -> EGResult<()> {
        if let Some(usage) = response.rate_limit_usage() {
            let _ = self.rate_limiters.set_usage(usage);
        }
        if let Some(retry_after_seconds) = response.retry_after() {
            let retry_after = Duration::from_secs(retry_after_seconds.0);
            let _ = self.rate_limiters.set_retry_after(retry_after);
            return Err(EGError::RateLimited);
        }
        Ok(())
    }
}

impl<Client> Connector<Client>
where
    Client: HttpClient,
{
    pub async fn sync_clock<SyncRequest, SyncResponse>(
        &self,
        sync_request: SyncRequest,
        timeout: Duration,
    ) -> EGResult<()>
    where
        SyncRequest: ServerTimeHttpRequest<SyncResponse>,
        SyncResponse: ETHttpResponse + ServerTimeResponse,
    {
        let costs = self.validate_rate_limits(&sync_request)?;
        let http_request = sync_request
            .try_into_http(self.signer.as_ref())
            .map_err(|e| EGError::External(Box::new(e)))?;
        let start = Instant::now();
        let response = match self.client.send(http_request, timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let round_trip_time = start.elapsed();
        let response = self.validate_http_status(response)?;
        let response = SyncResponse::try_from_http(response).map_err(|_| EGError::BadResponse)?;
        self.set_rate_limits(&response)?;
        if let Some(server_time) = response.server_time() {
            self.clock.sync(server_time.0 as i64, round_trip_time)?;
        }
        Ok(())
    }
    pub async fn send<Request, Response>(
        &self,
        request: Request,
        timeout: Duration,
    ) -> EGResult<Response>
    where
        Request: ETHttpRequest<Response = Response>,
        Response: ETHttpResponse,
    {
        let costs = self.validate_rate_limits(&request)?;
        let http_request = request
            .try_into_http(self.signer.as_ref())
            .map_err(|e| EGError::External(Box::new(e)))?;
        let response = match self.client.send(http_request, timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let response = self.validate_http_status(response)?;
        let response = Response::try_from_http(response).map_err(|_| EGError::BadResponse)?;
        self.set_rate_limits(&response)?;
        Ok(response)
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

impl<Client> Connector<(Client, Arc<WebsocketListener>)>
where
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> EGResult<()> {
        self.client.0.connect().await
    }
    pub fn is_connected(&self) -> EGResult<bool> {
        Ok(self.client.0.is_connected())
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        self.client.0.disconnect().await
    }
    pub async fn sync_clock<SyncRequest, SyncResponse>(
        &self,
        sync_request: SyncRequest,
        timeout: Duration,
    ) -> EGResult<()>
    where
        SyncRequest: ETWebsocketRequest<Response = SyncResponse>,
        SyncResponse: ETWebsocketResponse + ServerTimeResponse,
    {
        let costs = self.validate_rate_limits(&sync_request)?;
        let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
        let (websocket_request, response_matcher) = sync_request
            .try_into_websocket(self.signer.as_ref(), id)
            .map_err(|_| EGError::BadResponse)?;
        let start = Instant::now();
        let response = self
            .send_wait::<SyncRequest, SyncResponse>(
                websocket_request,
                costs,
                timeout,
                response_matcher,
            )
            .await?;
        let round_trip_time = start.elapsed();
        if let Some(server_time) = response.server_time() {
            self.clock.sync(server_time.0 as i64, round_trip_time)?;
        }
        Ok(())
    }
    pub async fn send<Request, Response>(
        &self,
        request: Request,
        timeout: Duration,
    ) -> EGResult<Response>
    where
        Request: ETWebsocketRequest<Response = Response>,
        Response: ETWebsocketResponse,
    {
        let costs = self.validate_rate_limits(&request)?;
        let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
        let (websocket_request, response_matcher) = request
            .try_into_websocket(self.signer.as_ref(), id)
            .map_err(|e| EGError::External(Box::new(e)))?;
        self.send_wait::<Request, Response>(websocket_request, costs, timeout, response_matcher)
            .await
    }
    async fn send_wait<Request, Response>(
        &self,
        message: String,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
        timeout: Duration,
        response_matcher: Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>,
    ) -> EGResult<Request::Response>
    where
        Request: ETWebsocketRequest<Response = Response>,
        Response: ETWebsocketResponse,
    {
        let waiter = self
            .client
            .1
            .waiter_for_filtered_response(response_matcher)?;
        let start = Instant::now();
        match self.client.0.send(message, timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let remaining = timeout.saturating_sub(start.elapsed());
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

impl<Client> Clone for Connector<Client> {
    fn clone(&self) -> Self {
        Self {
            rate_limiters: self.rate_limiters.clone(),
            clock: self.clock.clone(),
            signer: self.signer.clone(),
            client: self.client.clone(),
            auto_resync: self.auto_resync.clone(),
            resync: self.resync.clone(),
        }
    }
}

impl<Client> std::fmt::Debug for Connector<Client> {
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
