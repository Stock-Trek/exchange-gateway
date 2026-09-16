use crate::{
    clients::client::{HttpClient, WebsocketClient},
    clock::Clock,
    error::{EGError, EGResult},
    rate_limit::{
        rate_limiter::RateLimiter,
        rate_limiter_state::RateLimiterState,
        rate_limiters::{RateLimitGuard, RateLimiters},
    },
    retry_after::RetryAfter,
    submission::{Submission, SubmissionOutcome},
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
use tracing::{debug, warn};

#[cfg(feature = "auto-resync")]
use crate::auto_resync_connector::AutoResyncConnector;

#[cfg(feature = "iris")]
use {
    crate::clients::iris::IrisWebsocketClient,
    iris::{Config as IrisConfig, DisconnectedBehavior, ServerCloseBehavior},
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
    max_retries: u8,
    websocket_listener: Option<Arc<WebsocketListener>>,
}

impl Connector<(), ()> {
    pub fn try_new_http<Exchange, Client>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        client_creator: impl FnOnce(String) -> EGResult<Client>,
        request_timeout: Duration,
        max_retries: u8,
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
            max_retries,
            websocket_listener: None,
        })
    }
    #[cfg(feature = "reqwest")]
    pub fn try_new_http_reqwest<Exchange>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        request_timeout: Duration,
        max_retries: u8,
    ) -> EGResult<Connector<Exchange, ReqwestHttpClient>>
    where
        Exchange: ETExchange,
    {
        let client_creator = Box::new(move |url: String| ReqwestHttpClient::try_new(&url));
        Self::try_new_http(
            trading_mode,
            exchange,
            signer,
            client_creator,
            request_timeout,
            max_retries,
        )
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket<Exchange, Client>(
        trading_mode: TradingMode,
        exchange: Exchange,
        signer: Signer,
        client_creator: impl FnOnce(String, Arc<WebsocketListener>) -> EGResult<Client>,
        request_timeout: Duration,
        max_retries: u8,
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
        let client = client_creator(url, websocket_listener.clone())?;
        let rate_limiters = Self::rate_limiters(exchange.default_capacity())?;
        Ok(Connector {
            exchange,
            rate_limiters,
            clock: Clock::default(),
            signer,
            client,
            request_timeout,
            max_retries,
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
        max_retries: u8,
    ) -> EGResult<Connector<Exchange, IrisWebsocketClient>>
    where
        Exchange: ETExchange,
    {
        iris_config = iris_config
            .with_disconnected_behavior(DisconnectedBehavior::DropAllQueued)
            .with_server_close_behavior(ServerCloseBehavior::Reconnect);
        let client_creator = move |url: String, websocket_listener| {
            Ok(IrisWebsocketClient::with_config(
                &url,
                iris_config,
                websocket_listener,
            ))
        };
        Self::try_new_websocket(
            trading_mode,
            exchange,
            signer,
            client_creator,
            request_timeout,
            max_retries,
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
    fn refund_if_not_sent(
        &self,
        error: &EGError,
        costs: &mut Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<()> {
        if error.was_not_sent() {
            self.refund(std::mem::take(costs))?;
        }
        Ok(())
    }
    fn refund(&self, costs: Vec<(RateLimitRestriction, UsageCount)>) -> EGResult<()> {
        for (restriction, cost) in costs {
            self.rate_limiters.refund(restriction, cost)?;
        }
        Ok(())
    }
    fn on_send_failure<T>(
        &self,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<T> {
        if error.was_not_sent() {
            self.refund(costs)?;
        }
        Err(error)
    }
    fn set_rate_limits(&self, response: &impl ETResponse) -> EGResult<()> {
        if let Some(usage) = response.rate_limit_usage() {
            self.rate_limiters.set_usage(usage)?;
        }
        if let Some(retry_after_seconds) = response.retry_after() {
            let retry_after = Duration::from_secs(retry_after_seconds.0.max(0) as u64);
            self.rate_limiters.set_retry_after(retry_after)?;
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
        debug!(exchange = self.exchange.name(), "syncing clock over HTTP");
        let server_time_request = self.exchange.server_time_request_http();
        let costs = self.validate_rate_limits(&server_time_request)?;
        let http_request = match server_time_request.try_into_http(&self.signer) {
            Ok(http_request) => http_request,
            Err(error) => {
                self.refund(costs)?;
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
        let server_time = response
            .server_time()
            .map_err(|_| EGError::MissingServerTime)?;
        self.clock.sync(server_time, round_trip_time)?;
        debug!(
            exchange = self.exchange.name(),
            round_trip_ms = round_trip_time.as_millis() as u64,
            "clock synced over HTTP"
        );
        Ok(())
    }
    pub fn submit_http<'connector, Request>(
        &'connector self,
        request: Request,
    ) -> EGResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send + 'connector,
        Exchange: Sync,
        Client: Sync,
    {
        let costs = self
            .validate_rate_limits(&request)
            .map_err(EGError::send_not_sent)?;
        let guard = RateLimitGuard::new(&self.rate_limiters, costs.clone());
        let future = async move {
            guard.disarm();
            self.send_http(request, costs).await
        };
        Ok(Submission::new(future))
    }
    async fn send_http<Request>(
        &self,
        mut request: Request,
        mut costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest<Exchange = Exchange> + Send,
        Exchange: Sync,
        Client: Sync,
    {
        debug!(exchange = self.exchange.name(), "submitting HTTP request");
        let is_idempotent = request.is_idempotent();
        let is_signed = request.is_signed();
        let mut retries_remaining = if is_idempotent { self.max_retries } else { 0 };
        loop {
            let timestamp = match if is_signed {
                self.clock.server_time_estimate()
            } else {
                self.clock.server_time_estimate_unchecked()
            } {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    return self.submission_error_http(
                        request,
                        EGError::send_not_sent(error),
                        costs,
                    );
                }
            };
            request.set_timestamp(timestamp);
            let http_request = match request.clone().try_into_http(&self.signer) {
                Ok(http_request) => http_request,
                Err(error) => {
                    return self.submission_error_http(
                        request,
                        EGError::send_not_sent_external(error),
                        costs,
                    );
                }
            };
            let error = match self.client.send(http_request, self.request_timeout).await {
                Ok(http_response) => match self.handle_http_response(http_response) {
                    Ok(response) => return Ok(SubmissionOutcome::Submitted(response)),
                    Err(error) => error,
                },
                Err(error) => error,
            };
            if retries_remaining == 0 || !error.is_retryable() {
                return self.submission_error_http(request, error, costs);
            }
            retries_remaining -= 1;
            warn!(
                exchange = self.exchange.name(),
                retries_remaining,
                error = %error,
                "HTTP request failed, retrying"
            );
            self.refund_if_not_sent(&error, &mut costs)?;
            match self.validate_rate_limits(&request) {
                Ok(retry_costs) => costs = retry_costs,
                Err(_) => return self.submission_error_http(request, error, costs),
            }
        }
    }
    fn handle_http_response<Response>(&self, http_response: HttpResponse) -> EGResult<Response>
    where
        Response: ETHttpResponse,
    {
        let status = http_response.status;
        let retry_after = RetryAfter::from_headers(&http_response.headers);
        if status == 429 {
            if let Ok(response) = Response::try_from_http(http_response) {
                self.set_rate_limits(&response)
                    .map_err(EGError::send_failed)?;
            }
            if let Some(retry_after) = retry_after {
                self.rate_limiters
                    .set_retry_after(retry_after)
                    .map_err(EGError::send_failed)?;
            }
            warn!(
                exchange = self.exchange.name(),
                status, "exchange rate limited the request"
            );
            return Err(EGError::send_failed(EGError::RateLimited));
        }
        if !(200..300).contains(&status) {
            warn!(
                exchange = self.exchange.name(),
                status, "HTTP request failed"
            );
            if let Some(retry_after) = retry_after {
                self.rate_limiters
                    .set_retry_after(retry_after)
                    .map_err(EGError::send_unknown)?;
                // The exchange responded but is throttling us. The request reached
                // the exchange, so it cannot be reported as not sent.
                return Err(EGError::send_unknown(EGError::RateLimited));
            }
            let error = EGError::HttpError { status };
            return Err(if status >= 500 {
                EGError::send_unknown(error)
            } else {
                EGError::send_failed(error)
            });
        }
        if let Some(retry_after) = retry_after {
            self.rate_limiters
                .set_retry_after(retry_after)
                .map_err(EGError::send_unknown)?;
        }
        let response = Response::try_from_http(http_response)
            .map_err(|source| EGError::send_unknown(EGError::HttpParseError { source }))?;
        self.set_rate_limits(&response)
            .map_err(EGError::send_unknown)?;
        Ok(response)
    }
    fn submission_error_http<Request>(
        &self,
        request: Request,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETHttpRequest,
    {
        if error.has_unknown_response() {
            warn!(
                exchange = self.exchange.name(),
                error = %error,
                "request outcome unknown, verification required"
            );
            let verify = request.verification_request_http();
            let retry = if request.is_idempotent() {
                Some(request)
            } else {
                None
            };
            Ok(SubmissionOutcome::Unknown { retry, verify })
        } else {
            self.on_send_failure(error, costs)
        }
    }
}

impl<Exchange, Client> Connector<Exchange, Client>
where
    Exchange: ETExchange,
    Client: WebsocketClient,
{
    pub async fn connect(&self) -> EGResult<()> {
        debug!(exchange = self.exchange.name(), "connecting websocket");
        self.client.connect().await
    }
    pub fn is_connected(&self) -> EGResult<bool> {
        Ok(self.client.is_connected())
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        debug!(exchange = self.exchange.name(), "disconnecting websocket");
        self.client.disconnect().await
    }
    pub async fn sync_clock_websocket(&self) -> EGResult<()> {
        debug!(
            exchange = self.exchange.name(),
            "syncing clock over websocket"
        );
        let server_time_request = self.exchange.server_time_request_websocket();
        let costs = self.validate_rate_limits(&server_time_request)?;
        let id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
        let (websocket_request, response_matcher) =
            match server_time_request.try_into_websocket(&self.signer, id) {
                Ok(request) => request,
                Err(error) => {
                    self.refund(costs)?;
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
        let server_time = response
            .server_time()
            .map_err(|_| EGError::MissingServerTime)?;
        self.clock.sync(server_time, round_trip_time)?;
        debug!(
            exchange = self.exchange.name(),
            round_trip_ms = round_trip_time.as_millis() as u64,
            "clock synced over websocket"
        );
        Ok(())
    }
    pub fn submit_websocket<'connector, Request>(
        &'connector self,
        request: Request,
    ) -> EGResult<Submission<'connector, Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest<Exchange = Exchange> + Send + 'connector,
        Exchange: Sync,
    {
        let costs = self
            .validate_rate_limits(&request)
            .map_err(EGError::send_not_sent)?;
        let guard = RateLimitGuard::new(&self.rate_limiters, costs.clone());
        let future = async move {
            guard.disarm();
            self.send_websocket(request, costs).await
        };
        Ok(Submission::new(future))
    }
    async fn send_websocket<Request>(
        &self,
        mut request: Request,
        mut costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest<Exchange = Exchange> + Send,
        Exchange: Sync,
    {
        debug!(
            exchange = self.exchange.name(),
            "submitting websocket request"
        );
        let is_idempotent = request.is_idempotent();
        let is_signed = request.is_signed();
        let mut retries_remaining = if is_idempotent { self.max_retries } else { 0 };
        loop {
            let timestamp = match if is_signed {
                self.clock.server_time_estimate()
            } else {
                self.clock.server_time_estimate_unchecked()
            } {
                Ok(timestamp) => timestamp,
                Err(error) => {
                    return self.submission_error_websocket(
                        request,
                        EGError::send_not_sent(error),
                        costs,
                    );
                }
            };
            request.set_timestamp(timestamp);
            let websocket_id = ETWebsocketId::Str(uuid::Uuid::new_v4().to_string());
            let (websocket_request, response_matcher) = match request
                .clone()
                .try_into_websocket(&self.signer, websocket_id)
            {
                Ok(request) => request,
                Err(error) => {
                    return self.submission_error_websocket(
                        request,
                        EGError::send_not_sent_external(error),
                        costs,
                    );
                }
            };
            let error = match self.send_wait(websocket_request, response_matcher).await {
                Ok(response) => return Ok(SubmissionOutcome::Submitted(response)),
                Err(error) => error,
            };
            if retries_remaining == 0 || !error.is_retryable() {
                return self.submission_error_websocket(request, error, costs);
            }
            retries_remaining -= 1;
            warn!(
                exchange = self.exchange.name(),
                retries_remaining,
                error = %error,
                "websocket request failed, retrying"
            );
            self.refund_if_not_sent(&error, &mut costs)?;
            match self.validate_rate_limits(&request) {
                Ok(retry_costs) => costs = retry_costs,
                Err(_) => return self.submission_error_websocket(request, error, costs),
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
        let exchange = self.exchange.name();
        let start = Instant::now();
        debug!(exchange, "sending websocket request");
        self.client.send(message, self.request_timeout).await?;
        let remaining = self.request_timeout.saturating_sub(start.elapsed());
        let mut waiter = Box::pin(waiter);
        let mut delay = Box::pin(Delay::new(remaining));
        let response_value = poll_fn(move |cx| match waiter.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(EGError::send_unknown)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    warn!(exchange, "websocket request timed out");
                    Poll::Ready(Err(EGError::send_unknown(EGError::TimedOut)))
                }
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
    fn submission_error_websocket<Request>(
        &self,
        request: Request,
        error: EGError,
        costs: Vec<(RateLimitRestriction, UsageCount)>,
    ) -> EGResult<SubmissionOutcome<Request, Request::Response, Request::VerificationRequest>>
    where
        Request: ETWebsocketRequest,
    {
        if error.has_unknown_response() {
            warn!(
                exchange = self.exchange.name(),
                error = %error,
                "request outcome unknown, verification required"
            );
            let verify = request.verification_request_websocket();
            let retry = if request.is_idempotent() {
                Some(request)
            } else {
                None
            };
            Ok(SubmissionOutcome::Unknown { retry, verify })
        } else {
            self.on_send_failure(error, costs)
        }
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
