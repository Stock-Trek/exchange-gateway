use crate::{
    clients::client::WebsocketClient,
    error::{EGError, EGResult},
    listeners::listener::ListenerTrait,
    panic_guard::{catch_panic_async, panic_message},
};
use async_trait::async_trait;
use futures_timer::Delay;
use iris::{Client as IrisClient, Config as IrisConfig, ConnectionError, Listener as IrisListener};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

pub struct IrisWebsocketClient {
    client: IrisClient<String, serde_json::Value>,
}

impl IrisWebsocketClient {
    pub fn with_config(
        url: &str,
        config: IrisConfig,
        listener: Arc<dyn ListenerTrait<TMessage = serde_json::Value>>,
    ) -> Self {
        let client = IrisClient::new(
            config,
            Arc::new(IrisListenerAdapter { delegate: listener }),
            url,
        );
        Self { client }
    }
    async fn send_message_with_delay<D>(&self, message: String, delay: D) -> EGResult<()>
    where
        D: Future<Output = ()> + Send + 'static,
    {
        let mut send = Box::pin(self.client.send(message));
        let mut delay = Box::pin(delay);
        poll_fn(move |cx| match send.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result.map_err(Self::map_send_error)),
            Poll::Pending => match delay.as_mut().poll(cx) {
                Poll::Ready(()) => Poll::Ready(Err(EGError::TimedOut)),
                Poll::Pending => Poll::Pending,
            },
        })
        .await
    }
    fn map_send_error(error: ConnectionError) -> EGError {
        match error {
            ConnectionError::ConnectionClosed | ConnectionError::SendMessage(_) => {
                EGError::NotSent(Box::new(EGError::External(Box::new(error))))
            }
            error => EGError::External(Box::new(error)),
        }
    }
}

#[async_trait]
impl WebsocketClient for IrisWebsocketClient {
    async fn connect(&self) -> EGResult<()> {
        self.client
            .connect()
            .await
            .map_err(|e| EGError::External(Box::new(e)))
    }
    fn is_connected(&self) -> bool {
        self.client.is_connected()
    }
    async fn send(&self, message: String, timeout: Duration) -> EGResult<()> {
        self.send_message_with_delay(message, Delay::new(timeout))
            .await
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.client
            .disconnect()
            .await
            .map_err(|e| EGError::External(Box::new(e)))
    }
}

impl std::fmt::Debug for IrisWebsocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisWebsocketClient")
            .field("client", &"<websocket::WebsocketClient>")
            .finish()
    }
}

struct IrisListenerAdapter {
    delegate: Arc<dyn ListenerTrait<TMessage = serde_json::Value>>,
}

impl IrisListenerAdapter {
    /// Runs a user-code callback to completion, catching any panic so it can
    /// never unwind out of iris's connection task (which would kill the
    /// connection while `is_connected()` still reports `true`).
    ///
    /// Errors returned by the callback, as well as panics converted into
    /// errors, are reported to the delegate's `on_error`, which is itself
    /// guarded so that a panicking error handler cannot kill the task either.
    async fn run_guarded<F>(&self, future: F)
    where
        F: Future<Output = EGResult<()>>,
    {
        let result = match catch_panic_async(future).await {
            Ok(result) => result,
            Err(payload) => Err(EGError::CallbackPanicked(panic_message(payload.as_ref()))),
        };
        if let Err(error) = result {
            let _ = catch_panic_async(self.delegate.on_error(error)).await;
        }
    }
}

#[async_trait]
impl IrisListener<serde_json::Value> for IrisListenerAdapter {
    async fn on_connected(&self) {
        self.run_guarded(self.delegate.on_connected()).await;
    }
    async fn on_disconnected(&self) {
        self.run_guarded(self.delegate.on_disconnected()).await;
    }
    async fn on_message(&self, message: serde_json::Value) {
        self.run_guarded(self.delegate.on_message(message)).await;
    }
}

impl std::fmt::Debug for IrisListenerAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrisListenerAdapter")
            .field("delegate", &"<Listener>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct ErrorRecorder {
        reported: Mutex<Vec<String>>,
    }

    #[derive(Clone, Copy)]
    enum PanicIn {
        Connected,
        Disconnected,
        Message,
        Error,
    }

    struct TestDelegate {
        recorder: Arc<ErrorRecorder>,
        panic_in: Option<PanicIn>,
        message_error: Option<EGError>,
    }

    #[async_trait]
    impl ListenerTrait for TestDelegate {
        type TMessage = serde_json::Value;

        async fn on_connected(&self) -> EGResult<()> {
            if matches!(self.panic_in, Some(PanicIn::Connected)) {
                panic!("user on_connected panicked");
            }
            Ok(())
        }

        async fn on_disconnected(&self) -> EGResult<()> {
            if matches!(self.panic_in, Some(PanicIn::Disconnected)) {
                panic!("user on_disconnected panicked");
            }
            Ok(())
        }

        async fn on_error(&self, error: EGError) -> EGResult<()> {
            if matches!(self.panic_in, Some(PanicIn::Error)) {
                panic!("user on_error panicked");
            }
            self.recorder
                .reported
                .lock()
                .unwrap()
                .push(error.to_string());
            Ok(())
        }

        async fn on_message(&self, _message: serde_json::Value) -> EGResult<()> {
            if matches!(self.panic_in, Some(PanicIn::Message)) {
                panic!("user on_message panicked");
            }
            if self.message_error.is_some() {
                return Err(EGError::RateLimited);
            }
            Ok(())
        }
    }

    impl TestDelegate {
        fn adapter(panic_in: Option<PanicIn>) -> (IrisListenerAdapter, Arc<ErrorRecorder>) {
            let recorder = Arc::new(ErrorRecorder::default());
            let adapter = IrisListenerAdapter {
                delegate: Arc::new(TestDelegate {
                    recorder: recorder.clone(),
                    panic_in,
                    message_error: None,
                }),
            };
            (adapter, recorder)
        }

        fn adapter_with_error(message_error: EGError) -> (IrisListenerAdapter, Arc<ErrorRecorder>) {
            let recorder = Arc::new(ErrorRecorder::default());
            let adapter = IrisListenerAdapter {
                delegate: Arc::new(TestDelegate {
                    recorder: recorder.clone(),
                    panic_in: None,
                    message_error: Some(message_error),
                }),
            };
            (adapter, recorder)
        }
    }

    #[tokio::test]
    async fn panic_in_user_on_message_is_contained_and_reported() {
        let (adapter, recorder) = TestDelegate::adapter(Some(PanicIn::Message));

        // Must complete instead of unwinding (which would kill the connection
        // task) and must remain usable for subsequent messages.
        adapter.on_message(serde_json::json!({ "price": 1 })).await;
        adapter.on_message(serde_json::json!({ "price": 2 })).await;

        let reported = recorder.reported.lock().unwrap();
        assert_eq!(reported.len(), 2);
        assert!(
            reported
                .iter()
                .all(|r| r.contains("user on_message panicked")),
            "unexpected reports: {reported:?}"
        );
    }

    #[tokio::test]
    async fn panic_in_user_on_connected_is_contained_and_reported() {
        let (adapter, recorder) = TestDelegate::adapter(Some(PanicIn::Connected));

        adapter.on_connected().await;

        let reported = recorder.reported.lock().unwrap();
        assert_eq!(reported.len(), 1);
        assert!(reported[0].contains("user on_connected panicked"));
    }

    #[tokio::test]
    async fn panic_in_user_on_disconnected_is_contained_and_reported() {
        let (adapter, recorder) = TestDelegate::adapter(Some(PanicIn::Disconnected));

        adapter.on_disconnected().await;

        let reported = recorder.reported.lock().unwrap();
        assert_eq!(reported.len(), 1);
        assert!(reported[0].contains("user on_disconnected panicked"));
    }

    #[tokio::test]
    async fn panic_in_user_on_error_is_contained() {
        let (adapter, recorder) = TestDelegate::adapter(Some(PanicIn::Error));

        // on_message returns an error, which triggers on_error; the panic in
        // on_error must not escape either.
        adapter.on_message(serde_json::json!({ "price": 1 })).await;

        assert!(recorder.reported.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn error_returned_by_user_on_message_is_reported() {
        let (adapter, recorder) = TestDelegate::adapter_with_error(EGError::RateLimited);

        adapter.on_message(serde_json::json!({ "price": 1 })).await;

        let reported = recorder.reported.lock().unwrap();
        assert_eq!(reported.len(), 1);
        assert!(reported[0].contains("Rate limit exceeded"));
    }
}
