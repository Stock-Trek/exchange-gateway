use crate::error::{EGError, EGResult};
use std::{
    future::{Future, poll_fn},
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

#[derive(Default)]
pub struct AuthGate {
    state: Mutex<AuthGateState>,
}

#[derive(Default)]
pub struct AuthGateState {
    connection_epoch: u64,
    authenticated_epoch: u64,
    in_flight: Option<AuthInFlight>,
}

struct AuthInFlight {
    on_complete: AuthOnComplete,
    epoch: u64,
}

pub enum AuthGateAcquisition {
    Authenticator,
    Waiting(AuthOnComplete),
}

#[derive(Clone, Default)]
pub struct AuthOnComplete(Arc<Mutex<AuthCompletedState>>);

#[derive(Default)]
struct AuthCompletedState {
    done: bool,
    wakers: Vec<Waker>,
}

impl AuthGate {
    pub fn acquire(&self) -> EGResult<AuthGateAcquisition> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        match &state.in_flight {
            None => {
                state.in_flight = Some(AuthInFlight {
                    on_complete: AuthOnComplete::default(),
                    epoch: state.connection_epoch,
                });
                Ok(AuthGateAcquisition::Authenticator)
            }
            Some(in_flight) => Ok(AuthGateAcquisition::Waiting(in_flight.on_complete.clone())),
        }
    }

    pub fn release(&self) -> EGResult<()> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        let Some(in_flight) = state.in_flight.take() else {
            return Err(EGError::NotAuthenticated);
        };
        state.authenticated_epoch = in_flight.epoch;
        in_flight.on_complete.notify();
        Ok(())
    }

    pub fn cancel(&self) -> EGResult<()> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        let Some(in_flight) = state.in_flight.take() else {
            return Err(EGError::NotAuthenticated);
        };
        in_flight.on_complete.notify();
        Ok(())
    }

    pub fn on_connection_established(&self) -> EGResult<()> {
        self.bump_epoch()
    }

    pub fn on_connection_lost(&self) -> EGResult<()> {
        self.bump_epoch()
    }

    fn bump_epoch(&self) -> EGResult<()> {
        self.state
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .connection_epoch += 1;
        Ok(())
    }

    pub fn is_stale(&self) -> EGResult<bool> {
        let state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        Ok(state.connection_epoch != state.authenticated_epoch)
    }
}

impl std::fmt::Debug for AuthGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthGate").finish_non_exhaustive()
    }
}

impl AuthOnComplete {
    pub fn wait(&self) -> impl Future<Output = EGResult<()>> + Send + '_ {
        let on_complete = self.clone();
        poll_fn(move |cx| {
            let mut state = match on_complete.0.lock() {
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
    // The gate owns notification: release/cancel wake waiters themselves, so
    // callers never need to notify a handle they hold.
    fn notify(&self) {
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
    use std::time::Duration;

    #[test]
    fn epoch_is_fully_controlled_by_the_gate() {
        let gate = AuthGate::default();
        assert!(!gate.is_stale().unwrap());

        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.release().expect("Release must be Ok");
        assert!(!gate.is_stale().unwrap());

        gate.on_connection_established().unwrap();
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn losing_the_connection_invalidates_the_session() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.release().expect("Release must be Ok");
        assert!(!gate.is_stale().unwrap());

        // The connection is lost without a reconnect yet: the session bound
        // to it must be stale immediately so sends queued during the drop
        // cannot bypass re-authentication.
        gate.on_connection_lost().unwrap();
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn reconnect_during_authentication_is_detected() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };

        // The connection reconnects while authentication is running: the gate
        // records the completion against the acquire-time epoch, not the
        // current one, so the session is detected as stale.
        gate.on_connection_established().unwrap();
        gate.release().expect("Release must be Ok");
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn second_acquire_waits_for_in_flight_authentication() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        assert!(matches!(
            gate.acquire().unwrap(),
            AuthGateAcquisition::Waiting(_)
        ));
        gate.release().expect("Release must be Ok");
    }

    #[test]
    fn completing_without_an_in_flight_authentication_is_an_error() {
        let gate = AuthGate::default();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.release().expect("Release must be Ok");
        assert!(matches!(gate.release(), Err(EGError::NotAuthenticated)));
    }

    #[test]
    fn cancelling_a_failed_authentication_keeps_the_session_stale() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };

        // A failed authentication must not advance the authenticated epoch:
        // the session stays stale so a waiter retries against the current
        // connection instead of treating it as authenticated.
        gate.cancel().expect("Cancel must be Ok");
        assert!(gate.is_stale().unwrap());

        // A waiter can immediately re-acquire and retry.
        assert!(matches!(
            gate.acquire().unwrap(),
            AuthGateAcquisition::Authenticator
        ));
    }

    #[test]
    fn cancelling_without_an_in_flight_authentication_is_an_error() {
        let gate = AuthGate::default();
        assert!(matches!(gate.cancel(), Err(EGError::NotAuthenticated)));
    }

    #[tokio::test]
    async fn release_notifies_waiters() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        let AuthGateAcquisition::Waiting(on_complete) = gate.acquire().unwrap() else {
            panic!("expected a waiting acquisition");
        };
        let waiter = tokio::spawn(async move { on_complete.wait().await.unwrap() });
        gate.release().expect("Release must be Ok");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("release must wake the waiter")
            .expect("wait must be Ok");
    }

    #[tokio::test]
    async fn cancel_notifies_waiters() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        let AuthGateAcquisition::Waiting(on_complete) = gate.acquire().unwrap() else {
            panic!("expected a waiting acquisition");
        };
        let waiter = tokio::spawn(async move { on_complete.wait().await.unwrap() });
        gate.cancel().expect("Cancel must be Ok");
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancel must wake the waiter")
            .expect("wait must be Ok");
    }
}
