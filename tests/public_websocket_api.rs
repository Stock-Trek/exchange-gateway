use async_trait::async_trait;
use exchange_gateway::{
    clients::client::WebsocketClient, error::EGResult, prelude::WebsocketListener,
};
use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

struct CustomWebsocketClient {
    _listener: Arc<WebsocketListener>,
}

#[async_trait]
impl WebsocketClient for CustomWebsocketClient {
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

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn custom_client_can_feed_and_read_the_listener() {
    let listener = Arc::new(WebsocketListener::new());
    let _client = CustomWebsocketClient {
        _listener: listener.clone(),
    };
    let waiter = listener
        .waiter_for_filtered_response(Arc::new(|value: &serde_json::Value| value["id"] == 7))
        .unwrap();
    block_on(listener.on_message(serde_json::json!({ "id": 7, "value": 42 }))).unwrap();
    assert_eq!(
        block_on(waiter).unwrap(),
        serde_json::json!({ "id": 7, "value": 42 })
    );
}
