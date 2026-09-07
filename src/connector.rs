use crate::{
    clients::{
        client::{HttpClient, WebsocketClient},
        iris::IrisWebsocketClient,
        reqwest::ReqwestHttpClient,
    },
    clock::Clock,
    error::{EGError, EGResult},
    functions::{ArcTryConvertValue, BoxTryCreateOnce},
    listeners::{listener::ListenerTrait, websocket_listener::WebsocketListener},
    rate_limit::{
        rate_limiter::RateLimiter, rate_limiter_state::RateLimiterState,
        rate_limiters::RateLimiters,
    },
    server_time_response::ServerTimeResponse,
    urls::url,
};
use exchange_types::{
    new_types::UsageCount,
    rate_limited::{RateLimit, RateLimitRestriction, RateLimits},
    request::{ETHttpRequest, ETRequest, ETWebsocketRequest},
    response::{ETHttpResponse, ETResponse, ETWebsocketResponse},
    signer::Signer,
    urls::{Protocol, TradingMode, Urls},
    websocket_id::ETWebsocketId,
};
use futures_timer::Delay;
use iris::Config as IrisConfig;
use std::{
    collections::HashMap,
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};
use strum::IntoEnumIterator;

pub struct Connector<Client> {
    rate_limiters: RateLimiters,
    clock: Clock,
    signer: Signer,
    client: Arc<Client>,
}

impl<Client> Connector<Client> {
    pub fn try_new_http<C>(
        trading_mode: TradingMode,
        urls: &impl Urls,
        rate_limits: impl RateLimits,
        signer: Signer,
        client_creator: BoxTryCreateOnce<String, C>,
    ) -> EGResult<Connector<C>>
    where
        C: HttpClient,
    {
        let url = url(urls, Protocol::Http, trading_mode);
        let client = Arc::new(client_creator(url)?);
        Ok(Connector::<C> {
            rate_limiters: Self::rate_limiters(rate_limits),
            clock: Clock::default(),
            signer,
            client,
        })
    }
    #[cfg(feature = "reqwest")]
    pub fn try_new_http_reqwest(
        trading_mode: TradingMode,
        urls: &impl Urls,
        rate_limits: impl RateLimits,
        signer: Signer,
    ) -> EGResult<Connector<ReqwestHttpClient>> {
        let client_creator = Box::new(move |url: String| Ok(ReqwestHttpClient::new(&url)));
        Self::try_new_http(trading_mode, urls, rate_limits, signer, client_creator)
    }
    #[allow(clippy::type_complexity)]
    pub fn try_new_websocket<C, TransportRes>(
        trading_mode: TradingMode,
        urls: &impl Urls,
        rate_limits: impl RateLimits,
        signer: Signer,
        converter: ArcTryConvertValue<TransportRes, serde_json::Value>,
        listener: impl ListenerTrait<TMessage = serde_json::Value> + 'static,
        client_creator: BoxTryCreateOnce<
            (
                String,
                Arc<WebsocketListener<TransportRes, serde_json::Value>>,
            ),
            C,
        >,
    ) -> EGResult<Connector<(C, Arc<WebsocketListener<TransportRes, serde_json::Value>>)>>
    where
        C: WebsocketClient,
    {
        let websocket_listener = Arc::new(WebsocketListener::new(converter, listener));
        let url = url(urls, Protocol::Websocket, trading_mode);
        let client = client_creator((url, websocket_listener.clone()))?;
        Ok(
            Connector::<(C, Arc<WebsocketListener<TransportRes, serde_json::Value>>)> {
                rate_limiters: Self::rate_limiters(rate_limits),
                clock: Clock::new(),
                signer,
                client: Arc::new((client, websocket_listener)),
            },
        )
    }
    #[allow(clippy::type_complexity)]
    #[cfg(feature = "iris")]
    pub fn try_new_websocket_iris(
        trading_mode: TradingMode,
        urls: &impl Urls,
        rate_limits: impl RateLimits,
        signer: Signer,
        listener: impl ListenerTrait<TMessage = serde_json::Value> + 'static,
        iris_config: IrisConfig,
    ) -> EGResult<
        Connector<(
            IrisWebsocketClient,
            Arc<WebsocketListener<serde_json::Value, serde_json::Value>>,
        )>,
    > {
        let client_creator: BoxTryCreateOnce<
            (
                String,
                Arc<WebsocketListener<serde_json::Value, serde_json::Value>>,
            ),
            IrisWebsocketClient,
        > = Box::new(move |(url, websocket_listener)| {
            Ok(IrisWebsocketClient::with_config(
                &url,
                iris_config,
                websocket_listener,
            ))
        });
        let converter: ArcTryConvertValue<serde_json::Value, serde_json::Value> =
            Arc::new(|value: serde_json::Value| -> EGResult<serde_json::Value> { Ok(value) });
        Self::try_new_websocket(
            trading_mode,
            urls,
            rate_limits,
            signer,
            converter,
            listener,
            client_creator,
        )
    }
    /// Returns the exchange's estimated current server time in epoch
    /// milliseconds: the local clock corrected by the offset measured during
    /// the most recent `sync_clock`.
    ///
    /// Exchanges validate the timestamp embedded in every signed request
    /// against their own server clock and reject anything that falls outside
    /// the `recvWindow` (Binance's default is 5000 ms, error -1021), so the
    /// value returned here is only as trustworthy as the last sync: between
    /// syncs any local clock drift or jump directly eats into that window.
    /// Keep the clock current with `auto_sync_clock`, or re-sync on a cadence
    /// comfortably below the `recvWindow`, and use
    /// `duration_since_last_sync` to detect a stale clock. Until the first
    /// sync the offset is zero and this method returns unadjusted local time.
    pub fn server_time_millis(&self) -> EGResult<i64> {
        Ok(self.clock.now_millis())
    }
    /// Returns the clock offset in milliseconds: the value added to the local
    /// clock to estimate the exchange's server time, as measured by the most
    /// recent `sync_clock`. Zero until the first sync.
    pub fn offset_millis(&self) -> i64 {
        self.clock.offset_millis()
    }
    /// Returns how long it has been since the clock was last successfully
    /// synced (`sync_clock` / `auto_sync_clock`), or `Duration::MAX` if it
    /// has never been synced.
    ///
    /// Use this to detect a stale clock: once the age approaches the
    /// exchange's `recvWindow` (Binance default 5000 ms), the timestamps on
    /// signed requests risk rejection.
    pub fn duration_since_last_sync(&self) -> EGResult<Duration> {
        self.clock.duration_since_last_sync()
    }
    fn rate_limiters(rate_limits: impl RateLimits) -> RateLimiters {
        let default_capacity = rate_limits.default_capacity();
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
    fn validate_retry_after(&self, response: &impl ETResponse) -> EGResult<()> {
        if let Some(retry_after_seconds) = response.retry_after() {
            let retry_after = Duration::from_secs(retry_after_seconds.0);
            let _ = self.rate_limiters.retry_after(retry_after);
            Err(EGError::RateLimited)
        } else {
            Ok(())
        }
    }
}

impl<Client> Connector<Client>
where
    Client: HttpClient,
{
    /// Syncs the connector's clock with the exchange's server clock by
    /// sending a time request over this transport and measuring the
    /// round-trip time.
    ///
    /// Exchanges validate the timestamp embedded in every signed request
    /// against their own server clock and reject anything that falls outside
    /// the `recvWindow` (Binance's default is 5000 ms, error -1021). The
    /// offset measured here is what `server_time_millis` corrects for, but it
    /// decays as the local clock drifts or jumps, so a long-lived connector
    /// must re-sync on a cadence comfortably below the `recvWindow` (every
    /// 5-30 s keeps Binance's default window safe against typical drift).
    ///
    /// `auto_sync_clock` re-syncs automatically for the lifetime of the
    /// connector; `duration_since_last_sync` reports how stale the clock is.
    /// A response that carries no server time (for example an API-level
    /// error) leaves the clock untouched and returns the error.
    pub async fn sync_clock<SyncRequest, SyncResponse>(
        &self,
        sync_request: SyncRequest,
        timeout: Duration,
    ) -> EGResult<()>
    where
        SyncRequest: ETHttpRequest<Response = SyncResponse>,
        SyncResponse: ETHttpResponse + ServerTimeResponse,
    {
        let costs = self.validate_rate_limits(&sync_request)?;
        let http_request = sync_request
            .try_into_http(&self.signer)
            .map_err(|e| EGError::External(Box::new(e)))?;
        let start = Instant::now();
        let response = match self.client.send(http_request, timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let round_trip_time = start.elapsed();
        let response = SyncResponse::try_from_http(response).map_err(|_| EGError::BadResponse)?;
        self.validate_retry_after(&response)?;
        let server_time = response.server_time()?;
        self.clock.sync(server_time as i64, round_trip_time)
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
            .try_into_http(&self.signer)
            .map_err(|e| EGError::External(Box::new(e)))?;
        let response = match self.client.send(http_request, timeout).await {
            Ok(response) => response,
            Err(error) => return self.on_error(error, costs),
        };
        let response = Response::try_from_http(response).map_err(|_| EGError::BadResponse)?;
        self.validate_retry_after(&response)?;
        Ok(response)
    }
    /// Keeps the connector's clock synced with the exchange's server clock on
    /// a fixed cadence until the caller stops the task.
    ///
    /// The "set and forget" answer to drift: `make_sync_request` builds a
    /// fresh time request (for Binance, `BinanceTimeRequest::new`), it is
    /// sent with `timeout`, and after each successful sync the connector
    /// waits `interval` and syncs again. The first sync happens immediately,
    /// so spawning this right after construction bootstraps the offset before
    /// any signed traffic.
    ///
    /// The future ends only when a sync fails (the error is returned to the
    /// caller) or the task is dropped/cancelled, so run it on your own
    /// executor and restart it on error, e.g.:
    ///
    /// ```no_run
    /// # // Real code names its concrete HTTP client type, e.g. ReqwestHttpClient.
    /// # let connector: std::sync::Arc<
    /// #     exchange_gateway::connector::Connector<exchange_gateway::clients::reqwest::ReqwestHttpClient>,
    /// # > = unimplemented!();
    /// # tokio::spawn(async move {
    /// #     loop {
    /// #         if let Err(_error) = connector
    /// #             .auto_sync_clock(
    /// #                 exchange_types::binance::time::BinanceTimeRequest::new,
    /// #                 std::time::Duration::from_secs(10),
    /// #                 std::time::Duration::from_secs(30),
    /// #             )
    /// #             .await
    /// #         {
    /// #             // Log and restart; duration_since_last_sync() reports staleness meanwhile.
    /// #         }
    /// #     }
    /// # });
    /// ```
    ///
    /// Choose `interval` comfortably below the exchange's `recvWindow`:
    /// Binance rejects request timestamps more than `recvWindow` (default
    /// 5000 ms) from its server clock with error -1021, and any drift the
    /// local clock accumulates between syncs directly eats into that window.
    /// A 30 s interval keeps even a poor crystal (a few hundred ms/hour) well
    /// inside a 1000 ms window; shorten it (to a few seconds) only when the
    /// machine clock is unstable (jumpy NTP, VM pause/resume, laptop
    /// suspend). Each sync costs one request-weight unit, so sub-second
    /// intervals waste rate-limit budget without buying accuracy.
    pub async fn auto_sync_clock<SyncRequest, SyncResponse>(
        &self,
        make_sync_request: impl Fn() -> SyncRequest,
        timeout: Duration,
        interval: Duration,
    ) -> EGResult<()>
    where
        SyncRequest: ETHttpRequest<Response = SyncResponse>,
        SyncResponse: ETHttpResponse + ServerTimeResponse,
    {
        loop {
            self.sync_clock(make_sync_request(), timeout).await?;
            Delay::new(interval).await;
        }
    }
}

impl<Client, TransportRes>
    Connector<(
        Client,
        Arc<WebsocketListener<TransportRes, serde_json::Value>>,
    )>
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
    /// Syncs the connector's clock with the exchange's server clock by
    /// sending a time request over this transport and measuring the
    /// round-trip time.
    ///
    /// Exchanges validate the timestamp embedded in every signed request
    /// against their own server clock and reject anything that falls outside
    /// the `recvWindow` (Binance's default is 5000 ms, error -1021). The
    /// offset measured here is what `server_time_millis` corrects for, but it
    /// decays as the local clock drifts or jumps, so a long-lived connector
    /// must re-sync on a cadence comfortably below the `recvWindow` (every
    /// 5-30 s keeps Binance's default window safe against typical drift).
    ///
    /// `auto_sync_clock` re-syncs automatically for the lifetime of the
    /// connector; `duration_since_last_sync` reports how stale the clock is.
    /// A response that carries no server time (for example an API-level
    /// error) leaves the clock untouched and returns the error.
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
            .try_into_websocket(&self.signer, id)
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
        let server_time = response.server_time()?;
        self.clock.sync(server_time as i64, round_trip_time)
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
            .try_into_websocket(&self.signer, id)
            .map_err(|e| EGError::External(Box::new(e)))?;
        self.send_wait::<Request, Response>(websocket_request, costs, timeout, response_matcher)
            .await
    }
    /// Keeps the connector's clock synced with the exchange's server clock
    /// over the WebSocket transport on a fixed cadence until the caller stops
    /// the task.
    ///
    /// Same contract as the HTTP variant: `make_sync_request` builds a fresh
    /// time request, it is sent with `timeout`, and after each successful
    /// sync the connector waits `interval` and syncs again; the first sync is
    /// immediate. The future ends when a sync fails (returning the error) or
    /// the task is dropped/cancelled, so run it on your own executor and
    /// restart it on error.
    ///
    /// The WebSocket transport must be connected (`connect`) for each sync to
    /// succeed; when it reconnects after a drop, restart this task so the
    /// clock does not go stale. See the HTTP variant for guidance on choosing
    /// `interval` relative to the exchange's `recvWindow` skew window.
    pub async fn auto_sync_clock<SyncRequest, SyncResponse>(
        &self,
        make_sync_request: impl Fn() -> SyncRequest,
        timeout: Duration,
        interval: Duration,
    ) -> EGResult<()>
    where
        SyncRequest: ETWebsocketRequest<Response = SyncResponse>,
        SyncResponse: ETWebsocketResponse + ServerTimeResponse,
    {
        loop {
            self.sync_clock(make_sync_request(), timeout).await?;
            Delay::new(interval).await;
        }
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
        self.validate_retry_after(&response)?;
        Ok(response)
    }
}

impl<Client> std::fmt::Debug for Connector<Client> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorImpl")
            .field("rate_limits", &self.rate_limiters)
            .field("clock", &self.clock)
            .field("client", &"<client>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Connector;
    use crate::{
        clients::client::HttpClient,
        error::{EGError, EGResult},
    };
    use async_trait::async_trait;
    use exchange_types::{
        api_key_credential::ApiKeyCredentials,
        binance::{
            rate_limits::BinanceRateLimits, signer::SignerFactory, time::BinanceTimeRequest,
            urls::BinanceUrls,
        },
        http::{HttpRequest, HttpResponse},
        urls::TradingMode,
    };
    use serde_json::json;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[derive(Clone)]
    struct MockHttpClient {
        server_time_millis: Arc<AtomicI64>,
        time_request_count: Arc<AtomicUsize>,
        fail_next: Arc<AtomicBool>,
    }

    #[async_trait]
    impl HttpClient for MockHttpClient {
        async fn send(&self, request: HttpRequest, _timeout: Duration) -> EGResult<HttpResponse> {
            if request
                .query
                .as_deref()
                .is_some_and(|query| query.starts_with("time"))
            {
                self.time_request_count.fetch_add(1, Ordering::Relaxed);
                let body = if self.fail_next.swap(false, Ordering::Relaxed) {
                    serde_json::to_vec(&json!({
                        "code": -1021,
                        "msg": "Timestamp for this request is outside of the recvWindow."
                    }))
                    .expect("json")
                } else {
                    serde_json::to_vec(&json!({
                        "serverTime": self.server_time_millis.load(Ordering::Relaxed)
                    }))
                    .expect("json")
                };
                return Ok(HttpResponse {
                    status: 400,
                    body,
                    headers: vec![],
                });
            }
            Err(EGError::HttpError {
                status: 404,
                body: request.query.unwrap_or_default().into_bytes(),
            })
        }
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis() as i64
    }

    fn mock_connector(mock: MockHttpClient) -> EGResult<Connector<MockHttpClient>> {
        let credentials: ApiKeyCredentials = serde_json::from_value(json!({
            "api_key": "api-key",
            "secret": "secret",
        }))
        .expect("credentials");
        let signer = SignerFactory::hmac_sha256(credentials).expect("signer");
        let creator: crate::functions::BoxTryCreateOnce<String, MockHttpClient> =
            Box::new(move |_url| Ok(mock.clone()));
        Connector::<MockHttpClient>::try_new_http(
            TradingMode::Paper,
            &BinanceUrls,
            BinanceRateLimits,
            signer,
            creator,
        )
    }

    #[tokio::test]
    async fn accessors_report_unsynced_and_synced_states() -> EGResult<()> {
        let server_time = Arc::new(AtomicI64::new(now_millis() + 60_000));
        let connector = mock_connector(MockHttpClient {
            server_time_millis: server_time.clone(),
            time_request_count: Arc::new(AtomicUsize::new(0)),
            fail_next: Arc::new(AtomicBool::new(false)),
        })?;

        assert_eq!(connector.offset_millis(), 0);
        assert_eq!(connector.duration_since_last_sync()?, Duration::MAX);
        assert!((connector.server_time_millis()? - now_millis()).abs() < 1_000);

        connector
            .sync_clock(BinanceTimeRequest::new(), Duration::from_secs(5))
            .await?;

        assert!((connector.offset_millis() + 60_000).abs() < 2_000);
        assert!(connector.duration_since_last_sync()? < Duration::from_secs(5));
        assert!(
            (connector.server_time_millis()? - server_time.load(Ordering::Relaxed)).abs() < 2_000
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_clock_refreshes_the_offset_each_time() -> EGResult<()> {
        let server_time = Arc::new(AtomicI64::new(now_millis() + 60_000));
        let connector = mock_connector(MockHttpClient {
            server_time_millis: server_time.clone(),
            time_request_count: Arc::new(AtomicUsize::new(0)),
            fail_next: Arc::new(AtomicBool::new(false)),
        })?;

        connector
            .sync_clock(BinanceTimeRequest::new(), Duration::from_secs(5))
            .await?;
        let first_offset = connector.offset_millis();
        server_time.fetch_add(60_000, Ordering::Relaxed);
        connector
            .sync_clock(BinanceTimeRequest::new(), Duration::from_secs(5))
            .await?;
        let second_offset = connector.offset_millis();

        assert!((second_offset - first_offset + 60_000).abs() < 2_000);
        Ok(())
    }

    #[tokio::test]
    async fn sync_clock_reports_api_errors_and_leaves_the_clock_untouched() -> EGResult<()> {
        let server_time = Arc::new(AtomicI64::new(now_millis() + 60_000));
        let connector = mock_connector(MockHttpClient {
            server_time_millis: server_time,
            time_request_count: Arc::new(AtomicUsize::new(0)),
            fail_next: Arc::new(AtomicBool::new(true)),
        })?;

        let result = connector
            .sync_clock(BinanceTimeRequest::new(), Duration::from_secs(5))
            .await;
        assert!(matches!(result, Err(EGError::ApiError { code: -1021, .. })));
        assert_eq!(connector.offset_millis(), 0);
        assert_eq!(connector.duration_since_last_sync()?, Duration::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn auto_sync_clock_syncs_repeatedly_until_cancelled() -> EGResult<()> {
        let sync_count = Arc::new(AtomicUsize::new(0));
        let server_time = Arc::new(AtomicI64::new(now_millis() + 60_000));
        let connector = Arc::new(mock_connector(MockHttpClient {
            server_time_millis: server_time,
            time_request_count: sync_count.clone(),
            fail_next: Arc::new(AtomicBool::new(false)),
        })?);
        let auto_syncer = connector.clone();
        let handle = tokio::spawn(async move {
            auto_syncer
                .auto_sync_clock(
                    BinanceTimeRequest::new,
                    Duration::from_secs(5),
                    Duration::from_millis(20),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        handle.abort();
        let _ = handle.await;

        let syncs = sync_count.load(Ordering::Relaxed);
        assert!(syncs >= 3, "expected several automatic syncs, got {syncs}");
        assert!((connector.offset_millis() + 60_000).abs() < 2_000);
        Ok(())
    }
}
