//! Helpers for containing panics raised by user code so that they can never
//! unwind out of a task owned by this library.
//!
//! Listener and converter callbacks are user code. Without containment a panic
//! in such a callback would propagate up through the code that invoked it and
//! could kill an internal task entirely; for websocket connections that runs
//! inline on iris's connection task, leaving `is_connected()` true forever
//! while sends queue into a channel nobody is reading.

use std::{
    any::Any,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
};

/// Runs `f`, converting a panic into an `Err` containing the panic payload.
pub(crate) fn catch_panic<F, R>(f: F) -> Result<R, Box<dyn Any + Send>>
where
    F: FnOnce() -> R,
{
    catch_unwind(AssertUnwindSafe(f))
}

/// Polls `future` to completion, catching any panic raised while it is being
/// polled and returning it as an `Err` instead of unwinding the calling task.
///
/// A future that panicked mid-poll must not be polled again; this helper
/// guarantees that by returning `Err` and dropping the future.
pub(crate) async fn catch_panic_async<F, T>(future: F) -> Result<T, Box<dyn Any + Send>>
where
    F: Future<Output = T>,
{
    let mut future = std::pin::pin!(AssertUnwindSafe(future));
    poll_fn(|cx| match catch_panic(|| future.as_mut().poll(cx)) {
        Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
        Ok(Poll::Pending) => Poll::Pending,
        Err(payload) => Poll::Ready(Err(payload)),
    })
    .await
}

/// Renders a panic payload into a human-readable message.
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "Unknown panic cause".to_string()
    }
}
