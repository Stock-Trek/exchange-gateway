use crate::error::{EGError, EGResult};
use std::{
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

/// Serializes authentication so that at most one authentication runs at a
/// time: a caller that finds the session stale while another authentication is
/// in flight waits for it to finish instead of starting a second one. It also
/// owns the connection epoch: the transport reports (re)connections to the
/// gate, which bumps the epoch, and the session is stale whenever the epoch it
/// was authenticated against no longer matches the current one.
#[derive(Default)]
pub struct AuthGate {
    state: Mutex<AuthGateState>,
}

#[derive(Default)]
pub struct AuthGateState {
    /// The epoch of the current connection, bumped by [`AuthGate`] itself
    /// whenever the transport reports a (re)connection.
    connection_epoch: u64,
    /// The connection epoch the current authenticated session belongs to. The
    /// session is stale whenever this differs from `connection_epoch`.
    authenticated_epoch: u64,
    /// The in-flight authentication's completion signal, if one is running.
    in_flight: Option<AuthCompleted>,
}

pub enum AuthGateAcquisition {
    /// This caller is now the only task running authentication.
    Authenticator(AuthCompleted),
    /// Another caller is authenticating; wait for its completion signal.
    Waiting(AuthCompleted),
}

/// A one-shot signal that an in-flight authentication has finished, shared
/// between the authenticator and every caller waiting on it.
#[derive(Clone, Default)]
pub struct AuthCompleted(Arc<Mutex<AuthCompletedState>>);

#[derive(Default)]
struct AuthCompletedState {
    done: bool,
    wakers: Vec<Waker>,
}

impl AuthGate {
    /// Atomically marks the gate as busy, returning the completion signal the
    /// caller must notify when it finishes, or the in-flight signal to wait on
    /// if another authentication is already running.
    pub fn acquire(&self) -> EGResult<AuthGateAcquisition> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        match &state.in_flight {
            None => {
                let completed = AuthCompleted::default();
                state.in_flight = Some(completed.clone());
                Ok(AuthGateAcquisition::Authenticator(completed))
            }
            Some(completed) => Ok(AuthGateAcquisition::Waiting(completed.clone())),
        }
    }

    /// Returns the gate to idle once the running authentication finishes.
    pub fn release(&self, completed: &AuthCompleted) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if let Some(active) = &state.in_flight
            && Arc::ptr_eq(&active.0, &completed.0)
        {
            state.in_flight = None;
        }
    }

    /// Records that the transport established a (re)connection, bumping the
    /// connection epoch and thereby invalidating any session that was
    /// authenticated against an older one.
    pub fn on_connection_established(&self) -> EGResult<()> {
        self.state
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .connection_epoch += 1;
        Ok(())
    }

    /// The epoch of the current connection.
    pub fn connection_epoch(&self) -> EGResult<u64> {
        Ok(self
            .state
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .connection_epoch)
    }

    /// Whether the authenticated session belongs to an older connection epoch
    /// than the current one.
    pub fn is_stale(&self) -> EGResult<bool> {
        let state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        Ok(state.connection_epoch != state.authenticated_epoch)
    }

    /// Records the connection epoch the just-completed authentication ran on.
    pub fn set_authenticated_epoch(&self, epoch: u64) -> EGResult<()> {
        self.state
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .authenticated_epoch = epoch;
        Ok(())
    }
}

impl std::fmt::Debug for AuthGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthGate").finish_non_exhaustive()
    }
}

impl AuthCompleted {
    pub fn wait(&self) -> impl Future<Output = EGResult<()>> + Send + '_ {
        let completed = self.clone();
        poll_fn(move |cx| {
            let mut state = match completed.0.lock() {
                Ok(state) => state,
                Err(_) => return Poll::Ready(Err(EGError::MutexPoisoned)),
            };
            if state.done {
                Poll::Ready(Ok(()))
            } else {
                state.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        })
    }
    pub fn notify(&self) {
        let wakers = {
            let mut state = match self.0.lock() {
                Ok(state) => state,
                Err(_) => return,
            };
            state.done = true;
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}
