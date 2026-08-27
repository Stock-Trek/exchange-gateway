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
///
/// The epoch is entirely private to the gate and lives only in its state.
/// [`acquire`](AuthGate::acquire) records the connection epoch the
/// authentication must be recorded against in the gate's own in-flight record
/// and hands out an opaque [`AuthSession`] token; on success the caller hands
/// the token back to [`complete_authentication`](AuthGate::complete_authentication),
/// and the gate applies the epoch it recorded itself. Outside code never sees
/// an epoch value and can never supply one.
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
    /// The in-flight authentication, if one is running.
    in_flight: Option<AuthInFlight>,
}

/// A running authentication: its completion signal together with the
/// connection epoch it began against. The epoch is recorded here by the gate
/// at [`acquire`](AuthGate::acquire) time so the completed session can be
/// recorded against the same epoch even if the connection reconnects part way
/// through the authentication.
struct AuthInFlight {
    completed: AuthCompleted,
    epoch: u64,
}

pub enum AuthGateAcquisition {
    /// This caller is now the only task running authentication
    Authenticator(AuthCompleted),
    /// Another caller is authenticating; wait for its completion signal
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
    /// Atomically marks the gate as busy and records the connection epoch the
    /// authentication must be recorded against, returning the completion
    /// signal the caller must notify when it finishes together with the opaque
    /// session token to hand back on success (or the in-flight signal to wait
    /// on if another authentication is already running).
    pub fn acquire(&self) -> EGResult<AuthGateAcquisition> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        match &state.in_flight {
            None => {
                let completed = AuthCompleted::default();
                state.in_flight = Some(AuthInFlight {
                    completed: completed.clone(),
                    epoch: state.connection_epoch,
                });
                Ok(AuthGateAcquisition::Authenticator(completed))
            }
            Some(in_flight) => Ok(AuthGateAcquisition::Waiting(in_flight.completed.clone())),
        }
    }

    /// Returns the gate to idle once the running authentication finishes.
    pub fn release(&self, completed: &AuthCompleted) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if let Some(in_flight) = &state.in_flight
            && Arc::ptr_eq(&in_flight.completed.0, &completed.0)
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

    /// Whether the authenticated session belongs to an older connection epoch
    /// than the current one.
    pub fn is_stale(&self) -> EGResult<bool> {
        let state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        Ok(state.connection_epoch != state.authenticated_epoch)
    }

    /// Records that the authentication represented by `session` completed
    /// successfully: the authenticated session now belongs to the connection
    /// epoch the authentication began against, as recorded by the gate itself
    /// at [`acquire`](AuthGate::acquire) time.
    pub fn complete_authentication(&self) -> EGResult<()> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        let Some(in_flight) = &state.in_flight else {
            return Err(EGError::NotAuthenticated);
        };
        state.authenticated_epoch = in_flight.epoch;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_fully_controlled_by_the_gate() {
        let gate = AuthGate::default();
        assert!(!gate.is_stale().unwrap());

        // First connection: authenticating against epoch 1 is fresh...
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator(completed) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.complete_authentication().unwrap();
        gate.release(&completed);
        assert!(!gate.is_stale().unwrap());

        // ...but a reconnect bumps the epoch and invalidates the session even
        // though the token was completed against the old one.
        gate.on_connection_established().unwrap();
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn reconnect_during_authentication_is_detected() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator(completed) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };

        // The connection reconnects while authentication is running: the gate
        // records the completion against the acquire-time epoch, not the
        // current one, so the session is detected as stale.
        gate.on_connection_established().unwrap();
        gate.complete_authentication().unwrap();
        gate.release(&completed);
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn second_acquire_waits_for_in_flight_authentication() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator(completed) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        assert!(matches!(
            gate.acquire().unwrap(),
            AuthGateAcquisition::Waiting(_)
        ));
        gate.release(&completed);
    }

    #[test]
    fn completing_without_an_in_flight_authentication_is_an_error() {
        let gate = AuthGate::default();
        let AuthGateAcquisition::Authenticator(completed) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.release(&completed);
        assert!(matches!(
            gate.complete_authentication(),
            Err(EGError::NotAuthenticated)
        ));
    }
}
