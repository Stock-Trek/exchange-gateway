use crate::{
    clients::client::{HttpClient, WebsocketClient},
    clock::Clock,
    error::{EGError, EGResult},
    functions::BoxTryCreateOnce,
    rate_limit::{
        rate_limiter::RateLimiter, rate_limiter_state::RateLimiterState,
        rate_limiters::RateLimiters,
    },
    retry_after::RetryAfter,
    submission::{Submission, SubmissionId, SubmissionOutcome},
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

#[cfg(feature = "auto-resync")]
use crate::auto_resync_connector::AutoResyncConnector;

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
    max_retry_attempts: u8,
    websocket_listener: Option<Arc<WebsocketListener>>,
}

impl Connector<(), ()> {
    pub fn try_new_http<Exchange, Client>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        client_creator: BoxTryCreateOnce<String, Client>,
        request_timeout: Duration,
        max_retry_attempts: u8,
    ) -> EGResult<Connector<Exchange, Client>>
    where
        Exchange: ETExchange,
        Client: HttpClient + Send + Sync,
    {
        let url = exchange
            .urls()
            .env_var_or_default(exchange.name(), Protocol::Http, trading_mode);
        let client = client_creator(url)?;
        let rate_limiters = Self::rate_limiters(exchange.default_capacity())?;
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            max_retry_attempts,
            websocket_listener: None,
        })
    }
    #[cfg(feature = "reqwest")]
    pub fn try_new_http_reqwest<Exchange>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        request_timeout: Duration,
        max_retry_attempts: u8,
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
            max_retry_attempts,
        )
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket<Exchange, Client>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        client_creator: BoxTryCreateOnce<(String, Arc<WebsocketListener>), Client>,
        request_timeout: Duration,
        max_retry_attempts: u8,
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
        let rate_limiters = Self::rate_limiters(exchange.default_capacity())?;
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            max_retry_attempts,
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
        max_retry_attempts: u8,
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
            max_retry_attempts,
        )
    }
    fn rate_limiters(default_capacity: HashMap<RateLimit, UsageCount>) -> EGResult<RateLimiters> {
        let mut limiter_states = HashMap::new();
        for (rate_limit, capacity) in default_capacity {
            let RateLimit {
                restriction,
                interval_nanos,
            } = rate_limit;
            let states = limiter_states.entry(restriction).or_insert_with(Vec::new);
            let state = RateLimiterState::try_new(interval_nanos, capacity)?;
            states.push(state);
        }
        let limiters = limiter_states
            .iter()
            .map(|(restriction, states)| (*restriction, RateLimiter::new(states)))
            .collect::<HashMap<RateLimitRestriction, RateLimiter>>();
        Ok(RateLimiters::new(limiters))
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
    pub fn duration_since_last_sync(&self) -> EGResult<Option<Duration>> {
        self.clock.duration_since_last_sync()
    }
    pub fn remaining_rate_limit_capacity(&self) -> EGResult<HashMap<RateLimit, UsageCount>> {
        self.rate_limiters.remaining_capacity()
    }
    pub fn server_time_estimate(&self) -> EGResult<Milliseconds> {
        self.clock.server_time_estimate()
    }
    fn validate_rate_limits<Request>(
        &self,
        request: &Request,
    ) -> EGResult<Vec<(RateLimitRestriction, UsageCount)>>
    where
        Request: ETRequest,
    {
        let costs = RateLimitRestriction::iter()
            .filter_map(|restriction| {
                let cost = request.rate_limit_usage(restriction);
                (cost > UsageCount::ZERO).then_some((restriction, cost))
            })
            .collect::<Vec<_>>();
        self.rate_limiters.did_acquire(&costs)?;
        Ok(costs)
    }
    fn refund(&self, costs: Vec<(RateLimitRestriction, UsageCount)>) {
        for (restriction, cost) in costs {
            let _ = self.rate_limiters.refund(restriction, cost);
        }
    }
    fn on_send_failure<T>(
        &self,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<T> {
        if error.was_not_sent() {
            self.refund(costs);
        }
        Err(error)
    }
    fn finish_submission<Response>(
        &self,
        id: &SubmissionId,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Response>> {
        if error.is_unknown() {
            Ok(SubmissionOutcome::Indeterminate(id.clone()))
        } else {
            self.on_send_failure(error, costs)
        }
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
                return Err(EGError::external(error));
            }
        };
        let start = Instant::now();
        let http_response = match self.client.send(http_request, self.request_timeout).await {
            Ok(http_response) => http_response,
            Err(error) => return self.on_send_failure(error, costs),
        };
        let round_trip_time = start.elapsed();
        let response =
            self.handle_http_response::<Exchange::ServerTimeResponseHttp>(http_response)?;
        let server_time = response.server_time().ok_or(EGError::MissingServerTime)?;
        self.clock.sync(server_time, round_trip_time)?;
        Ok(())
    }
    pub fn submit_http<'a, Response>(
        &'a self,
        request: impl ETHttpRequest<Exchange = Exchange, Response = Response> + Clone + 'a,
    ) -> EGResult<Submission<'a, Response>>
    where
        Response: ETHttpResponse + 'a,
    {
        let costs = self
            .validate_rate_limits(&request)
            .map_err(EGError::send_not_sent)?;
        let id = SubmissionId::new();
        let future = self.send_http(request, id.clone(), costs);
        Ok(Submission::new(id, future))
    }
    async fn send_http<Response>(
        &self,
        mut request: impl ETHttpRequest<Exchange = Exchange, Response = Response> + Clone,
        id: SubmissionId,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Response>>
    where
        Response: ETHttpResponse,
    {
        let is_idempotent = request.is_idempotent();
        let is_signed = request.is_signed();
        let mut retries_remaining = if is_idempotent {
            self.max_retry_attempts
        } else {
            0
        };
        loop {
            let timestamp = match if is_signed {
                self.clock.server_time_estimate()
            } else {
                self.clock.server_time_estimate_unchecked()
            } {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    return self.finish_submission(&id, EGError::send_not_sent(error), costs);
                }
            };
            request.set_timestamp(timestamp);
            let http_request = match request.clone().try_into_http(&self.signer) {
                Ok(http_request) => http_request,
                Err(error) => {
                    return self.finish_submission(
                        &id,
                        EGError::send_not_sent_external(error),
                        costs,
                    );
                }
            };
            let error = match self.client.send(http_request, self.request_timeout).await {
                Ok(http_response) => match self.handle_http_response::<Response>(http_response) {
                    Ok(response) => return Ok(SubmissionOutcome::Confirmed(response)),
                    Err(error) => error,
                },
                Err(error) => error,
            };
            if retries_remaining == 0 || !error.is_retryable() {
                return self.finish_submission(&id, error, costs);
            }
            retries_remaining -= 1;
            if error.was_not_sent() {
                self.rate_limiters.did_acquire(&costs)?;
            }
        }
    }
    fn handle_http_response<Response>(&self, http_response: HttpResponse) -> EGResult<Response>
    where
        Response: ETHttpResponse,
    {
        if let Some(retry_after) = RetryAfter::from_headers(&http_response.headers) {
            let _ = self.rate_limiters.set_retry_after(retry_after);
            // The exchange responded but is throttling us. The request reached the
            // exchange, so it cannot be reported as not sent; a 2xx carrying a
            // Retry-After may even have succeeded, so the outcome is unknown.
            return Err(EGError::send_unknown(EGError::RateLimited));
        }
        if http_response.status == 429 {
            return Err(EGError::send_failed(EGError::RateLimited));
        }
        if !(200..300).contains(&http_response.status) {
            return Err(EGError::send_failed(EGError::HttpError {
                status: http_response.status,
                body: http_response.body,
            }));
        }
        let response = Response::try_from_http(http_response)
            .map_err(|source| EGError::send_unknown(EGError::HttpParseError { source }))?;
        self.set_rate_limits(&response)
            .map_err(EGError::send_unknown)?;
        Ok(response)
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
                Err(error) => {
                    self.refund(costs);
                    return Err(EGError::external(error));
                }
            };
        let start = Instant::now();
        let response: Exchange::ServerTimeResponseWebsocket =
            match self.send_wait(websocket_request, response_matcher).await {
                Ok(response) => response,
                Err(error) => return self.on_send_failure(error, costs),
            };
        let round_trip_time = start.elapsed();
        let server_time = response.server_time().ok_or(EGError::MissingServerTime)?;
        self.clock.sync(server_time, round_trip_time)?;
        Ok(())
    }
    pub fn submit_websocket<'a, Response>(
        &'a self,
        request: impl ETWebsocketRequest<Exchange = Exchange, Response = Response> + Clone + 'a,
    ) -> EGResult<Submission<'a, Response>>
    where
        Response: ETWebsocketResponse + 'a,
    {
        let costs = self
            .validate_rate_limits(&request)
            .map_err(EGError::send_not_sent)?;
        let id = SubmissionId::new();
        let future = self.send_websocket(request, id.clone(), costs);
        Ok(Submission::new(id, future))
    }
    async fn send_websocket<Response>(
        &self,
        mut request: impl ETWebsocketRequest<Exchange = Exchange, Response = Response> + Clone,
        id: SubmissionId,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Response>>
    where
        Response: ETWebsocketResponse,
    {
        let websocket_id: ETWebsocketId = id.clone().into();
        let is_idempotent = request.is_idempotent();
        let is_signed = request.is_signed();
        let mut retries_remaining = if is_idempotent {
            self.max_retry_attempts
        } else {
            0
        };
        loop {
            let timestamp = match if is_signed {
                self.clock.server_time_estimate()
            } else {
                self.clock.server_time_estimate_unchecked()
            } {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    return self.finish_submission(&id, EGError::send_not_sent(error), costs);
                }
            };
            request.set_timestamp(timestamp);
            let (websocket_request, response_matcher) = match request
                .clone()
                .try_into_websocket(&self.signer, websocket_id.clone())
            {
                Ok(request) => request,
                Err(error) => {
                    return self.finish_submission(
                        &id,
                        EGError::send_not_sent_external(error),
                        costs,
                    );
                }
            };
            let error = match self.send_wait(websocket_request, response_matcher).await {
                Ok(response) => return Ok(SubmissionOutcome::Confirmed(response)),
                Err(error) => error,
            };
            if retries_remaining == 0 || !error.is_retryable() {
                return self.finish_submission(&id, error, costs);
            }
            retries_remaining -= 1;
            if error.was_not_sent() {
                self.rate_limiters.did_acquire(&costs)?;
            }
        }
    }
    async fn send_wait<Response>(
        &self,
        message: String,
        response_matcher: Arc<dyn Fn(&serde_json::Value) -> bool + Send + Sync>,
    ) -> EGResult<Response>
    where
        Response: ETWebsocketResponse,
    {
        let listener = match self.websocket_listener.as_ref() {
            Some(listener) => listener,
            None => {
                return Err(EGError::send_not_sent(EGError::WebsocketListenerMissing));
            }
        };
        let waiter = match listener.waiter_for_filtered_response(response_matcher) {
            Ok(waiter) => waiter,
            Err(error) => {
                return Err(EGError::send_not_sent(error));
            }
        };
        let start = Instant::now();
        self.client.send(message, self.request_timeout).await?;
        let remaining = self.request_timeout.saturating_sub(start.elapsed());
        let mut waiter = Box::pin(waiter);
        let mut delay = Box::pin(Delay::new(remaining));
        let response_value = poll_fn(move |cx| match waiter.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(EGError::send_unknown)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::send_unknown(EGError::TimedOut))),
                Poll::Pending => Poll::Pending,
            },
        })
        .await?;
        let response = Response::try_from_websocket(response_value)
            .map_err(|source| EGError::send_unknown(EGError::WebsocketParseError { source }))?;
        self.set_rate_limits(&response)
            .map_err(EGError::send_unknown)?;
        Ok(response)
    }
}

impl<Exchange, Client> std::fmt::Debug for Connector<Exchange, Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("rate_limiters", &self.rate_limiters)
            .field("clock", &self.clock)
            .field("signer", &"<signer>")
            .field("client", &"<client>")
            .finish()
    }
}
