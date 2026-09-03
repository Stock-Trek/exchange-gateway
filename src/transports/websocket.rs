use crate::{
    error::{EGError, EGResult},
    functions::{ArcPredicate, ArcTryConvertValue, TryConvertValue},
    listeners::websocket_listener::WebsocketListener,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use futures_timer::Delay;
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

#[async_trait]
pub trait WebsocketClientTrait: Send + Sync {
    type TransportReq;
    type TransportRes;

    async fn connect(&self) -> EGResult<()>;
    fn is_connected(&self) -> bool;
    async fn send_message(&self, message: Self::TransportReq, timeout: Duration) -> EGResult<()>;
    async fn disconnect(&self) -> EGResult<()>;
}

pub(crate) struct WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes> {
    client: Arc<dyn WebsocketClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>>,
    convert_request: TryConvertValue<EGReq, TransportReq>,
    convert_response: ArcTryConvertValue<TransportRes, EGRes>,
    websocket_listener: Arc<WebsocketListener<TransportRes, EGRes>>,
}

#[async_trait]
impl<EGReq, TransportReq, TransportRes, EGRes>
    TransportTrait<EGReq, TransportReq, TransportRes, EGRes>
    for WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    EGRes: Send + Sync + 'static,
{
    fn try_convert_request(&self, request: EGReq) -> EGResult<TransportReq> {
        (self.convert_request)(request)
    }
    fn try_convert_response(&self, response: TransportRes) -> EGResult<EGRes> {
        (self.convert_response)(response)
    }
    async fn connect(&self) -> EGResult<()> {
        self.client.connect().await
    }
    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }
    async fn send_and_wait_for(
        &self,
        request: EGReq,
        timeout: Duration,
        filter: ArcPredicate<EGRes>,
    ) -> EGResult<EGRes> {
        let transport_req = self.try_convert_request(request)?;
        let waiter = self
            .websocket_listener
            .waiter_for_filtered_response(filter)?;
        self.client
            .send_message(transport_req, timeout)
            .await
            .map_err(|error| match error {
                EGError::TimedOut => error,
                error => EGError::NotSent(Box::new(error)),
            })?;
        self.wait_for_response(waiter, timeout).await
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.client.disconnect().await
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes>
    WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>
where
    EGRes: Send + Sync + 'static,
{
    pub fn new(
        client: Arc<
            dyn WebsocketClientTrait<TransportReq = TransportReq, TransportRes = TransportRes>,
        >,
        convert_request: TryConvertValue<EGReq, TransportReq>,
        convert_response: ArcTryConvertValue<TransportRes, EGRes>,
        websocket_listener: Arc<WebsocketListener<TransportRes, EGRes>>,
    ) -> Self {
        Self {
            client,
            convert_request,
            convert_response,
            websocket_listener,
        }
    }
    fn wait_for_response(
        &self,
        waiter: impl Future<Output = EGResult<EGRes>> + Send + 'static,
        timeout: Duration,
    ) -> impl Future<Output = EGResult<EGRes>> + Send + 'static {
        let mut waiter = Box::pin(waiter);
        let mut delay = Box::pin(Delay::new(timeout));
        poll_fn(move |cx| match waiter.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebsocketTransport")
            .field("client", &"<HttpClientTrait>")
            .field("convert_request", &"<function>")
            .field("convert_response", &"<function>")
            .field("websocket_listener", &self.websocket_listener)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        clock::Synchronization,
        connector::Connector,
        connector_impl::ConnectorImpl,
        error::EGResult,
        listeners::listener::ListenerTrait,
        rate_limit::{
            feedback::RateLimitFeedback, rate_limit_config::RateLimitConfig,
            rate_limit_type::RateLimitType, rate_limiter::RateLimiter, rate_limits::RateLimits,
        },
        transports::transport::Transport,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    };

    #[derive(Default)]
    struct RecordingDelegate {
        received: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait]
    impl ListenerTrait for RecordingDelegate {
        type TMessage = u64;

        async fn on_message(&self, message: u64) -> EGResult<()> {
            self.received
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?
                .push(message);
            Ok(())
        }
    }

    /// A client whose send times out while the request is already on the
    /// wire: `send_message` returns `TimedOut` immediately, and the matching
    /// response is delivered to the listener only once the test releases it.
    struct TimeoutClient {
        listener: Arc<dyn ListenerTrait<TMessage = u64>>,
        connected: Arc<AtomicBool>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl WebsocketClientTrait for TimeoutClient {
        type TransportReq = u64;
        type TransportRes = u64;

        async fn connect(&self) -> EGResult<()> {
            self.connected.store(true, Ordering::SeqCst);
            self.listener.on_connected().await
        }
        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }
        async fn send_message(
            &self,
            message: Self::TransportReq,
            _timeout: Duration,
        ) -> EGResult<()> {
            // The request is on the wire, but the send is reported as timed
            // out. The matching response arrives only once released.
            let listener = self.listener.clone();
            let release = self.release.clone();
            tokio::spawn(async move {
                release.notified().await;
                let _ = listener.on_message(message).await;
            });
            Err(EGError::TimedOut)
        }
        async fn disconnect(&self) -> EGResult<()> {
            self.connected.store(false, Ordering::SeqCst);
            self.listener.on_disconnected().await
        }
    }

    #[tokio::test]
    async fn send_timeout_swallows_the_late_response() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let listener = Arc::new(WebsocketListener::new(
            Arc::new(Ok),
            |_: &u64| Ok(RateLimitFeedback::default()),
            RateLimits {
                weight: RateLimiter::new(vec![]),
                orders: RateLimiter::new(vec![]),
            },
            RecordingDelegate {
                received: received.clone(),
            },
        ));
        let client = Arc::new(TimeoutClient {
            listener: listener.clone(),
            connected: Arc::new(AtomicBool::new(false)),
            release: release.clone(),
        });
        let transport = WebsocketTransport::new(client, Ok, Arc::new(Ok), listener);
        transport.connect().await.expect("connect should succeed");

        // The send times out, so the caller sees `TimedOut` and its waiter is
        // dropped ...
        let result = transport
            .send_and_wait_for(
                7,
                Duration::from_secs(1),
                Arc::new(|response: &u64| *response == 7),
            )
            .await;
        assert!(matches!(result, Err(EGError::TimedOut)));

        // ... but the request was already on the wire: the matching response
        // that arrives afterwards must be swallowed, not forwarded to the
        // delegate as if it were a push.
        release.notify_one();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            received.lock().unwrap().is_empty(),
            "the late response to a timed-out send must not leak to the delegate"
        );
    }

    struct RefusingClient {
        attempts: Arc<AtomicU32>,
    }

    #[async_trait]
    impl WebsocketClientTrait for RefusingClient {
        type TransportReq = u64;
        type TransportRes = u64;

        async fn connect(&self) -> EGResult<()> {
            Ok(())
        }
        fn is_connected(&self) -> bool {
            false
        }
        async fn send_message(&self, _message: u64, _timeout: Duration) -> EGResult<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            Err(EGError::NotConnected)
        }
        async fn disconnect(&self) -> EGResult<()> {
            Ok(())
        }
    }

    struct SendThenDisconnectClient {
        listener: Arc<dyn ListenerTrait<TMessage = u64>>,
        attempts: Arc<AtomicU32>,
    }

    #[async_trait]
    impl WebsocketClientTrait for SendThenDisconnectClient {
        type TransportReq = u64;
        type TransportRes = u64;

        async fn connect(&self) -> EGResult<()> {
            Ok(())
        }
        fn is_connected(&self) -> bool {
            false
        }
        async fn send_message(&self, _message: u64, _timeout: Duration) -> EGResult<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            self.listener.on_disconnected().await
        }
        async fn disconnect(&self) -> EGResult<()> {
            Ok(())
        }
    }

    fn listener_with(limits: RateLimits) -> Arc<WebsocketListener<u64, u64>> {
        Arc::new(WebsocketListener::new(
            Arc::new(Ok),
            |_: &u64| Ok(RateLimitFeedback::default()),
            limits,
            RecordingDelegate::default(),
        ))
    }

    fn connector_with_client(
        listener: Arc<WebsocketListener<u64, u64>>,
        client: Arc<dyn WebsocketClientTrait<TransportReq = u64, TransportRes = u64>>,
        limits: RateLimits,
    ) -> ConnectorImpl<u64, u64, u64, u64> {
        ConnectorImpl::new(
            limits,
            Synchronization {
                create_time_request: || 0,
                timeout: Duration::from_secs(5),
                to_server_time: |_: &u64| Ok(0),
            },
            |_: &u64| 1,
            |_: &u64| 0,
            |_: &u64| -> ArcPredicate<u64> { Arc::new(|_: &u64| true) },
            Transport::Websocket(WebsocketTransport::new(client, Ok, Arc::new(Ok), listener)),
        )
    }

    fn single_slot_rate_limits() -> RateLimits {
        RateLimits {
            weight: RateLimiter::new(vec![RateLimitConfig {
                rate_limit_type: RateLimitType::RequestWeight,
                capacity_per_interval: 1,
                interval_nanos: Duration::from_secs(60).as_nanos(),
            }]),
            orders: RateLimiter::new(vec![]),
        }
    }

    #[tokio::test]
    async fn refused_send_is_reported_as_not_sent() {
        let attempts = Arc::new(AtomicU32::new(0));
        let transport = WebsocketTransport::new(
            Arc::new(RefusingClient {
                attempts: attempts.clone(),
            }),
            Ok,
            Arc::new(Ok),
            listener_with(RateLimits {
                weight: RateLimiter::new(vec![]),
                orders: RateLimiter::new(vec![]),
            }),
        );
        let error = transport
            .send_and_wait_for(1, Duration::from_secs(1), Arc::new(|_: &u64| true))
            .await
            .expect_err("a refused send must fail");
        match error {
            EGError::NotSent(inner) => assert!(matches!(*inner, EGError::NotConnected)),
            other => panic!("expected NotSent, got: {other:?}"),
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn send_refunds_local_reservation_when_the_request_was_never_sent() {
        let limits = single_slot_rate_limits();
        let attempts = Arc::new(AtomicU32::new(0));
        let client = Arc::new(RefusingClient {
            attempts: attempts.clone(),
        });
        let connector =
            connector_with_client(listener_with(limits.clone()), client, limits.clone());
        let error = connector
            .send(1, Duration::from_secs(1))
            .await
            .expect_err("a refused send must fail");
        assert!(matches!(error, EGError::NotSent(_)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let error = connector
            .send(1, Duration::from_secs(1))
            .await
            .expect_err("the refunded reservation must let the second send reach the transport");
        assert!(matches!(error, EGError::NotSent(_)));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn send_keeps_local_reservation_when_the_connection_drops_after_the_send() {
        let limits = single_slot_rate_limits();
        let attempts = Arc::new(AtomicU32::new(0));
        let listener = listener_with(limits.clone());
        let client = Arc::new(SendThenDisconnectClient {
            listener: listener.clone(),
            attempts: attempts.clone(),
        });
        let connector = connector_with_client(listener, client, limits);
        let error = connector
            .send(1, Duration::from_secs(1))
            .await
            .expect_err("the waiter must fail with NotConnected when the connection drops");
        assert!(matches!(error, EGError::NotConnected));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let error = connector.send(1, Duration::from_secs(1)).await.expect_err(
            "a sent request keeps its reservation, so the second send is rejected locally",
        );
        assert!(matches!(error, EGError::RateLimited(_)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
