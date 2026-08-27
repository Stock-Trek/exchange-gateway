use crate::error::{EGError, EGResult};
use std::{
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

/// Serializes authentication so that at most one authentication runs at a
/// time: a caller that finds the session stale while another authentication is
/// in flight waits for it to finish instead of starting a second one.
#[derive(Default)]
pub struct AuthGate {
    state: Mutex<AuthGateState>,
}

#[derive(Default)]
pub enum AuthGateState {
    #[default]
    Idle,
    Authenticating(AuthCompleted),
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
        match &*state {
            AuthGateState::Idle => {
                let completed = AuthCompleted::default();
                *state = AuthGateState::Authenticating(completed.clone());
                Ok(AuthGateAcquisition::Authenticator(completed))
            }
            AuthGateState::Authenticating(completed) => {
                Ok(AuthGateAcquisition::Waiting(completed.clone()))
            }
        }
    }

    /// Returns the gate to idle once the running authentication finishes.
    pub fn release(&self, completed: &AuthCompleted) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if let AuthGateState::Authenticating(active) = &*state
            && Arc::ptr_eq(&active.0, &completed.0)
        {
            *state = AuthGateState::Idle;
        }
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
