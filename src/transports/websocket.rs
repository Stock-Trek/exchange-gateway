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
pub(crate) trait WebsocketClientTrait: Send + Sync {
    type TransportReq;
    type TransportRes;
    /// The concrete error type produced by the transport (e.g.
    /// `iris::ConnectionError`), carried by [`EGError::External`] instead of
    /// being boxed into a trait object.
    type Error: std::error::Error + Send + Sync + 'static;

    async fn connect(&self) -> EGResult<(), Self::Error>;
    fn is_connected(&self) -> bool;
    async fn send_message(
        &self,
        message: Self::TransportReq,
        timeout: Duration,
    ) -> EGResult<(), Self::Error>;
    async fn disconnect(&self) -> EGResult<(), Self::Error>;
}

pub(crate) struct WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes, E> {
    client: Arc<
        dyn WebsocketClientTrait<
                TransportReq = TransportReq,
                TransportRes = TransportRes,
                Error = E,
            >,
    >,
    convert_request: TryConvertValue<EGReq, TransportReq>,
    convert_response: ArcTryConvertValue<TransportRes, EGRes>,
    websocket_listener: Arc<WebsocketListener<TransportRes, EGRes>>,
}

#[async_trait]
impl<EGReq, TransportReq, TransportRes, EGRes, E>
    TransportTrait<EGReq, TransportReq, TransportRes, EGRes>
    for WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes, E>
where
    EGReq: Send,
    EGRes: Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn try_convert_request(&self, request: EGReq) -> EGResult<TransportReq> {
        (self.convert_request)(request)
    }
    fn try_convert_response(&self, response: TransportRes) -> EGResult<EGRes> {
        (self.convert_response)(response)
    }
    async fn connect(&self) -> EGResult<()> {
        self.client
            .connect()
            .await
            .map_err(EGError::into_boxed_external)
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
            .map_err(EGError::into_boxed_external)?;
        self.wait_for_response(waiter, timeout).await
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.client
            .disconnect()
            .await
            .map_err(EGError::into_boxed_external)
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes, E>
    WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes, E>
where
    EGRes: Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    pub fn new(
        client: Arc<
            dyn WebsocketClientTrait<
                    TransportReq = TransportReq,
                    TransportRes = TransportRes,
                    Error = E,
                >,
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

impl<EGReq, TransportReq, TransportRes, EGRes, E> std::fmt::Debug
    for WebsocketTransport<EGReq, TransportReq, TransportRes, EGRes, E>
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
        auth_gate::AuthGate,
        error::EGResult,
        listeners::listener::ListenerTrait,
        rate_limit::{
            feedback::RateLimitFeedback, rate_limiter::RateLimiter, rate_limits::RateLimits,
        },
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
        type Error = std::io::Error;

        async fn connect(&self) -> EGResult<(), Self::Error> {
            self.connected.store(true, Ordering::SeqCst);
            self.listener
                .on_connected()
                .await
                .map_err(|error| error.map_external(std::io::Error::other))
        }
        fn is_connected(&self) -> bool {
            self.connected.load(Ordering::SeqCst)
        }
        async fn send_message(
            &self,
            message: Self::TransportReq,
            _timeout: Duration,
        ) -> EGResult<(), Self::Error> {
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
        async fn disconnect(&self) -> EGResult<(), Self::Error> {
            self.connected.store(false, Ordering::SeqCst);
            self.listener
                .on_disconnected()
                .await
                .map_err(|error| error.map_external(std::io::Error::other))
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
            Arc::new(AuthGate::default()),
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
}
