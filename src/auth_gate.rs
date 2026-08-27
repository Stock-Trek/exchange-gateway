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
    Authenticator(AuthOnComplete),
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
                let on_complete = AuthOnComplete::default();
                state.in_flight = Some(AuthInFlight {
                    on_complete: on_complete.clone(),
                    epoch: state.connection_epoch,
                });
                Ok(AuthGateAcquisition::Authenticator(on_complete))
            }
            Some(in_flight) => Ok(AuthGateAcquisition::Waiting(in_flight.on_complete.clone())),
        }
    }

    pub fn release(&self) -> EGResult<()> {
        let mut state = self.state.lock().map_err(|_| EGError::MutexPoisoned)?;
        let Some(in_flight) = &state.in_flight.take() else {
            return Err(EGError::NotAuthenticated);
        };
        state.authenticated_epoch = in_flight.epoch;
        Ok(())
    }

    pub fn on_connection_established(&self) -> EGResult<()> {
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

        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator(on_complete) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.release().expect("Release must be Ok");
        on_complete.notify();
        assert!(!gate.is_stale().unwrap());

        gate.on_connection_established().unwrap();
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn reconnect_during_authentication_is_detected() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator(on_complete) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };

        // The connection reconnects while authentication is running: the gate
        // records the completion against the acquire-time epoch, not the
        // current one, so the session is detected as stale.
        gate.on_connection_established().unwrap();
        gate.release().expect("Release must be Ok");
        on_complete.notify();
        assert!(gate.is_stale().unwrap());
    }

    #[test]
    fn second_acquire_waits_for_in_flight_authentication() {
        let gate = AuthGate::default();
        gate.on_connection_established().unwrap();
        let AuthGateAcquisition::Authenticator(on_complete) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        assert!(matches!(
            gate.acquire().unwrap(),
            AuthGateAcquisition::Waiting(_)
        ));
        gate.release().expect("Release must be Ok");
        on_complete.notify();
    }

    #[test]
    fn completing_without_an_in_flight_authentication_is_an_error() {
        let gate = AuthGate::default();
        let AuthGateAcquisition::Authenticator(on_complete) = gate.acquire().unwrap() else {
            panic!("expected an authenticator acquisition");
        };
        gate.release().expect("Release must be Ok");
        on_complete.notify();
        assert!(matches!(gate.release(), Err(EGError::NotAuthenticated)));
    }
}
