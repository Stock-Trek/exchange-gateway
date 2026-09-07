use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};

pub struct PanicUtils;

impl PanicUtils {
    /// Runs `f`, converting a panic into an `Err` containing the panic payload
    pub(crate) fn catch_panic<F, R>(f: F) -> Result<R, Box<dyn Any + Send>>
    where
        F: FnOnce() -> R,
    {
        catch_unwind(AssertUnwindSafe(f))
    }
    /// Polls `future` to completion, catching any panic raised while it is being polled and returning it as an `Err`
    pub(crate) async fn catch_panic_async<F, T>(future: F) -> Result<T, Box<dyn Any + Send>>
    where
        F: Future<Output = T>,
    {
        let mut future = std::pin::pin!(AssertUnwindSafe(future));
        poll_fn(|cx| match Self::catch_panic(|| future.as_mut().poll(cx)) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        })
        .await
    }
    /// Renders a panic payload into a human-readable message
    pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_string()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            "Unknown panic cause".to_string()
        }
    }
}
