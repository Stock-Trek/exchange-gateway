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
    pub fn server_time_millis(&self) -> EGResult<i64> {
        Ok(self.clock.now_millis())
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
        let server_time = response.server_time() as i64;
        self.clock.sync(server_time, round_trip_time)
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
        let server_time = response.server_time() as i64;
        self.clock.sync(server_time, round_trip_time)
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
        // Await the matching response only for the remainder of the timeout so
        // the whole send-and-wait is bounded like an HTTP send instead of
        // hanging indefinitely (and holding rate-limit tokens) when the
        // exchange never replies with a matching id.
        let remaining = timeout.saturating_sub(start.elapsed());
        let response_value = wait_for_response(waiter, remaining).await?;
        let response = Response::try_from_websocket(response_value)
            .map_err(|e| EGError::External(Box::new(e)))?;
        self.validate_retry_after(&response)?;
        Ok(response)
    }
}

async fn wait_for_response<F, T>(waiter: F, timeout: Duration) -> EGResult<T>
where
    F: Future<Output = EGResult<T>> + Send,
{
    let mut waiter = Box::pin(waiter);
    let mut delay = Box::pin(Delay::new(timeout));
    poll_fn(move |cx| match waiter.as_mut().poll(cx) {
        Poll::Ready(result) => Poll::Ready(result),
        Poll::Pending => match delay.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
            Poll::Pending => Poll::Pending,
        },
    })
    .await
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
    use super::*;
    use async_trait::async_trait;
    use exchange_types::{
        encode::ByteEncoder,
        encrypt::Encryptor,
        error::{ETError, ETResult},
        new_types::{Seconds, UsageCount},
        rate_limited::{RateLimit, RateLimitRestriction, RateLimits, RateUsage},
        request::{ETRequest, ETWebsocketRequest, WebsocketResponseMatcher},
        response::{ETResponse, ETWebsocketResponse},
        signer::Signer,
        urls::{Protocol, TradingMode, Urls},
        websocket_id::ETWebsocketId,
    };
    use serde::Serialize;

    struct TestUrls;
    impl Urls for TestUrls {
        fn name(&self) -> &'static str {
            "test"
        }
        fn url(&self, _protocol: Protocol, _trading_mode: TradingMode) -> &str {
            "wss://example.test"
        }
    }

    struct TestRateLimits;
    impl RateLimits for TestRateLimits {
        fn default_capacity(&self) -> HashMap<RateLimit, UsageCount> {
            HashMap::new()
        }
    }

    #[derive(Clone)]
    struct MockWebsocketClient;

    #[async_trait]
    impl WebsocketClient for MockWebsocketClient {
        async fn connect(&self) -> EGResult<()> {
            Ok(())
        }
        fn is_connected(&self) -> bool {
            true
        }
        async fn send(&self, _message: String, _timeout: Duration) -> EGResult<()> {
            Ok(())
        }
        async fn disconnect(&self) -> EGResult<()> {
            Ok(())
        }
    }

    struct NoOpListener;
    #[async_trait]
    impl ListenerTrait for NoOpListener {
        type TMessage = serde_json::Value;
        async fn on_message(&self, _message: serde_json::Value) -> EGResult<()> {
            Ok(())
        }
    }

    #[derive(Debug, Serialize)]
    struct TestRequest;

    impl ETRequest for TestRequest {
        fn rate_limit_usage(&self, _restriction: RateLimitRestriction) -> UsageCount {
            UsageCount::ZERO
        }
        fn is_signed(&self) -> bool {
            false
        }
        fn set_api_key(&mut self, _api_key: Option<String>) {}
        fn query_params(&self, _percent_encode: bool) -> String {
            String::new()
        }
    }

    #[derive(Debug)]
    struct TestResponse;

    impl ETResponse for TestResponse {
        fn rate_limit_usage(&self) -> Option<&HashMap<RateLimit, RateUsage>> {
            None
        }
        fn retry_after(&self) -> Option<Seconds> {
            None
        }
    }

    impl ETWebsocketRequest for TestRequest {
        type Response = TestResponse;

        fn method_name(&self) -> &'static str {
            "test.method"
        }
        fn try_into_websocket(
            self,
            _signer: &Signer,
            id: ETWebsocketId,
        ) -> ETResult<(String, WebsocketResponseMatcher)> {
            let matcher_id = id.clone();
            let matcher: WebsocketResponseMatcher = Arc::new(move |value: &serde_json::Value| {
                value.get("id").is_some_and(|id_value| match &matcher_id {
                    ETWebsocketId::Int(expected) => id_value.as_i64() == Some(*expected),
                    ETWebsocketId::Str(expected) => id_value.as_str() == Some(expected),
                    _ => false,
                })
            });
            let id_json = serde_json::to_string(&id).map_err(ETError::SerializeRequest)?;
            let message = format!(r#"{{"id":{id_json},"method":"test.method"}}"#);
            Ok((message, matcher))
        }
    }

    impl ETWebsocketResponse for TestResponse {
        fn try_from_websocket(_response: serde_json::Value) -> ETResult<Self> {
            Ok(TestResponse)
        }
    }

    fn test_signer() -> Signer {
        let secret_key = ed25519_compact::SecretKey::new([7u8; 64]);
        Signer::new(
            "test-api-key".to_string(),
            Encryptor::Ed25519(secret_key),
            ByteEncoder::HexLower,
        )
    }

    type TestConnector = Connector<(
        MockWebsocketClient,
        Arc<WebsocketListener<serde_json::Value, serde_json::Value>>,
    )>;

    fn test_connector() -> TestConnector {
        let converter: ArcTryConvertValue<serde_json::Value, serde_json::Value> =
            Arc::new(|value: serde_json::Value| -> EGResult<serde_json::Value> { Ok(value) });
        TestConnector::try_new_websocket::<MockWebsocketClient, serde_json::Value>(
            TradingMode::Real,
            &TestUrls,
            TestRateLimits,
            test_signer(),
            converter,
            NoOpListener,
            Box::new(|(_url, _listener)| Ok(MockWebsocketClient)),
        )
        .expect("test connector should be constructible")
    }

    #[tokio::test]
    async fn send_times_out_when_the_exchange_never_responds() {
        let connector = test_connector();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            connector.send(TestRequest, Duration::from_millis(100)),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(EGError::TimedOut))),
            "expected send to time out waiting for a response, got {result:?}"
        );
    }
}
