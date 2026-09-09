use crate::error::{EGError, EGResult};
use async_trait::async_trait;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        mpsc::{self, RecvTimeoutError, Sender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

#[doc(hidden)]
#[async_trait]
pub trait Resync<SyncRequest, SyncResponse> {
    async fn resync(&self, request: SyncRequest, timeout: Duration) -> EGResult<()>;
}

pub(crate) type ResyncFuture = Pin<Box<dyn Future<Output = EGResult<()>> + Send>>;
pub(crate) type ResyncFn = Arc<dyn Fn() -> ResyncFuture + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct AutoResync {
    state: Arc<Mutex<Option<AutoResyncHandle>>>,
}

struct AutoResyncHandle {
    sender: Sender<ControlMsg>,
    join: JoinHandle<()>,
}

enum ControlMsg {
    SetDuration(Duration),
    Stop,
}

impl AutoResync {
    pub(crate) fn start_or_update<F, Fut>(&self, duration: Duration, mut resync: F) -> EGResult<()>
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = EGResult<()>>,
    {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        if let Some(handle) = state.as_ref()
            && handle
                .sender
                .send(ControlMsg::SetDuration(duration))
                .is_ok()
        {
            return Ok(());
        }
        *state = None;
        let (sender, receiver) = mpsc::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| EGError::External(Box::new(e)))?;
        let join = thread::Builder::new()
            .name("exchange-gateway-auto-resync".into())
            .spawn(move || {
                let mut current = duration;
                loop {
                    match receiver.recv_timeout(current) {
                        Ok(ControlMsg::SetDuration(new_duration)) => current = new_duration,
                        Ok(ControlMsg::Stop) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {
                            let _ = runtime.block_on(resync());
                        }
                    }
                }
            })
            .map_err(|e| EGError::External(Box::new(e)))?;
        *state = Some(AutoResyncHandle { sender, join });
        Ok(())
    }

    pub(crate) fn stop(&self) -> EGResult<()> {
        let handle = self
            .state
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .take();
        match handle {
            Some(handle) => {
                let _ = handle.sender.send(ControlMsg::Stop);
                drop(handle.sender);
                match handle.join.join() {
                    Ok(()) => Ok(()),
                    Err(_) => Err(EGError::AutoResyncClockPanicked),
                }
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn start_update_and_stop() {
        let auto_resync = AutoResync::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        auto_resync
            .start_or_update(Duration::from_millis(20), move || {
                counter.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Ok(()))
            })
            .unwrap();
        std::thread::sleep(Duration::from_millis(70));
        auto_resync.stop().unwrap();
        let calls_before = calls.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(calls_before, calls.load(Ordering::SeqCst));
        assert!(calls_before >= 2);
    }
}
