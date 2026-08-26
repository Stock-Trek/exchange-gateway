use crate::{
    authenticate_leg::AuthenticateLeg,
    connector::Connector,
    error::{EGError, EGResult},
    functions::{ArcTryConvertValue, TryConvertRef},
    rate_limit::rate_limits::RateLimits,
    sign::{
        convert_signer::ConvertSigner,
        signer::{Signer, SignerTrait},
    },
    transports::transport::{Transport, TransportTrait},
};
use async_trait::async_trait;
use std::{
    future::{Future, poll_fn},
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Poll, Waker},
    time::Duration,
};

pub struct ConnectorImpl<
    ExternalReq,
    EGUnsignedReq,
    TCredentials,
    EGReq,
    TransportReq,
    TransportRes,
    EGRes,
> {
    rate_limits: RateLimits,
    to_weight: fn(&EGUnsignedReq) -> u32,
    to_order_count: fn(&EGUnsignedReq) -> u32,
    to_unsigned_request: ArcTryConvertValue<ExternalReq, EGUnsignedReq>,
    sync_timestamp: ArcTryConvertValue<EGUnsignedReq, EGUnsignedReq>,
    transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
    null_signer: ConvertSigner<EGUnsignedReq, EGReq>,
    credentials: Option<TCredentials>,
    create_signer: TryConvertRef<TCredentials, Signer<EGUnsignedReq, EGReq>>,
    authenticate_legs: Vec<AuthenticateLeg<EGUnsignedReq, EGReq, EGRes>>,
    signer: Arc<Mutex<Option<Signer<EGUnsignedReq, EGReq>>>>,
    authenticated_epoch: Arc<AtomicU64>,
    auth_gate: AuthGate,
}

#[async_trait]
impl<
    ExternalReq,
    EGUnsignedReq,
    TCredentials,
    EGReq,
    TransportReq,
    TransportRes,
    EGRes,
    ExternalRes,
> Connector<ExternalReq, ExternalRes>
    for ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
    >
where
    ExternalReq: Send,
    TCredentials: Sync,
    EGReq: Send,
    TransportRes: Send,
    TransportReq: Send,
    EGRes: Send + Sync + 'static,
{
    async fn connect(&self) -> EGResult<()> {
        self.transport.connect().await?;
        self.authenticate().await
    }
    fn is_connected(&self) -> EGResult<bool> {
        Ok(self.transport.is_connected())
    }
    fn is_authenticated(&self) -> EGResult<bool> {
        if !self.transport.is_connected() {
            return Ok(false);
        }
        let guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
        Ok((*guard).is_some() && !self.session_is_stale())
    }
    async fn disconnect(&self) -> EGResult<()> {
        {
            let mut guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            *guard = None;
        }
        self.transport.disconnect().await
    }
    async fn send(&self, request: ExternalReq, signed: bool, timeout: Duration) -> EGResult<()> {
        if signed && self.session_is_stale() {
            self.authenticate().await?;
        }
        let (signed_request, weight, order_count) = {
            let unsigned = (self.to_unsigned_request)(request)?;
            let unsigned = if signed {
                (self.sync_timestamp)(unsigned)?
            } else {
                unsigned
            };
            self.check_rate_limits(&unsigned)?;
            let weight = (self.to_weight)(&unsigned);
            let order_count = (self.to_order_count)(&unsigned);
            let signed_request = match self.signed_request(unsigned, signed) {
                Ok(signed_request) => signed_request,
                Err(error) => {
                    let _ = self.rate_limits.refund(weight, order_count);
                    return Err(error);
                }
            };
            (signed_request, weight, order_count)
        };
        match self
            .transport
            .fire_and_forget(signed_request, timeout)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self.rate_limits.refund(weight, order_count);
                Err(error)
            }
        }
    }
}
impl<ExternalReq, EGUnsignedReq, TCredentials, EGReq, TransportReq, TransportRes, EGRes>
    ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
    >
where
    EGReq: Send,
    EGRes: Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        rate_limits: RateLimits,
        to_weight: fn(&EGUnsignedReq) -> u32,
        to_order_count: fn(&EGUnsignedReq) -> u32,
        to_unsigned_request: ArcTryConvertValue<ExternalReq, EGUnsignedReq>,
        sync_timestamp: ArcTryConvertValue<EGUnsignedReq, EGUnsignedReq>,
        transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
        null_signer: ConvertSigner<EGUnsignedReq, EGReq>,
        credentials: Option<TCredentials>,
        create_signer: TryConvertRef<TCredentials, Signer<EGUnsignedReq, EGReq>>,
        authenticate_legs: Vec<AuthenticateLeg<EGUnsignedReq, EGReq, EGRes>>,
    ) -> Self {
        Self {
            rate_limits,
            to_weight,
            to_order_count,
            to_unsigned_request,
            sync_timestamp,
            transport,
            null_signer,
            credentials,
            create_signer,
            authenticate_legs,
            signer: Arc::new(Mutex::new(None)),
            authenticated_epoch: Arc::new(AtomicU64::new(0)),
            auth_gate: AuthGate::default(),
        }
    }
    /// Runs the authentication legs against the current connection and records
    /// the connection epoch the resulting session belongs to. Only one task
    /// may be inside this method at a time (see [`AuthGate`]).
    async fn authenticate(&self) -> EGResult<()> {
        let Some(credentials) = &self.credentials else {
            return Ok(());
        };
        loop {
            // A concurrent caller may have re-authenticated while we waited.
            if !self.session_is_stale() {
                return Ok(());
            }
            let completed = match self.auth_gate.acquire()? {
                AuthGateAcquisition::Waiting(completed) => {
                    // Another authentication is already in flight: wait for it
                    // to finish instead of starting a second one, then re-check
                    // whether the session is still stale.
                    completed.wait().await?;
                    continue;
                }
                AuthGateAcquisition::Authenticator(completed) => completed,
            };
            let epoch = self.transport.connection_epoch();
            let result = self.run_authentication(credentials, epoch).await;
            // Clear the gate before waking waiters so a waiter that finds the
            // session stale can immediately become the next authenticator.
            self.auth_gate.release(&completed);
            completed.notify();
            match result {
                Err(error) => return Err(error),
                Ok(()) => {
                    // The connection reconnected while we were authenticating:
                    // the session was established on a connection that is no
                    // longer current, so try again against the new one.
                    if self.session_is_stale() {
                        continue;
                    }
                    return Ok(());
                }
            }
        }
    }
    /// Sends each authentication leg and, on success, installs the resulting
    /// signer for `epoch`. If the connection reconnects part way through, the
    /// signer is installed anyway (keyed to `epoch`) and the caller detects
    /// the staleness via [`Self::session_is_stale`].
    async fn run_authentication(
        &self,
        credentials: &TCredentials,
        epoch: u64,
    ) -> EGResult<()> {
        let mut signer = (self.create_signer)(credentials)?;
        for leg in &self.authenticate_legs {
            let (signed_auth_message, weight, order_count) = {
                let auth_message = (leg.create_auth_message)();
                self.check_rate_limits(&auth_message)?;
                let weight = (self.to_weight)(&auth_message);
                let order_count = (self.to_order_count)(&auth_message);
                let signed_auth_message = match signer.sign(auth_message) {
                    Ok(signed_auth_message) => signed_auth_message,
                    Err(error) => {
                        let _ = self.rate_limits.refund(weight, order_count);
                        return Err(error);
                    }
                };
                (signed_auth_message, weight, order_count)
            };
            let authentication_response = match self
                .transport
                .send_and_wait_for(signed_auth_message, leg.timeout, leg.filter.clone())
                .await
            {
                Ok(authentication_response) => authentication_response,
                Err(error) => {
                    let _ = self.rate_limits.refund(weight, order_count);
                    return Err(error);
                }
            };
            signer = (leg.create_signer)(authentication_response)?;
        }
        {
            let mut guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            *guard = Some(signer);
        }
        self.authenticated_epoch.store(epoch, Ordering::Relaxed);
        Ok(())
    }
    fn session_is_stale(&self) -> bool {
        !self.authenticate_legs.is_empty()
            && self.transport.connection_epoch() != self.authenticated_epoch.load(Ordering::Relaxed)
    }
    fn signed_request(&self, unsigned: EGUnsignedReq, signed: bool) -> EGResult<EGReq> {
        if signed {
            let guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            match guard.deref() {
                None => Err(EGError::NotAuthenticated),
                Some(signer) => signer.sign(unsigned),
            }
        } else {
            self.null_signer.sign(unsigned)
        }
    }
    fn check_rate_limits(&self, unsigned: &EGUnsignedReq) -> EGResult<()> {
        let weight = (self.to_weight)(unsigned);
        if !self.rate_limits.weight.did_acquire(weight)? {
            return Err(EGError::RateLimited);
        }
        let order_count = (self.to_order_count)(unsigned);
        if !self.rate_limits.orders.did_acquire(order_count)? {
            // Roll back the weight we already acquired for this request.
            let _ = self.rate_limits.weight.refund(weight);
            return Err(EGError::RateLimited);
        }
        Ok(())
    }
}

impl<ExternalReq, EGUnsignedReq, TCredentials, EGReq, TransportReq, TransportRes, EGRes>
    std::fmt::Debug
    for ConnectorImpl<
        ExternalReq,
        EGUnsignedReq,
        TCredentials,
        EGReq,
        TransportReq,
        TransportRes,
        EGRes,
    >
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("rate_limits", &self.rate_limits)
            .field("to_weight", &"<function>")
            .field("to_order_count", &"<function>")
            .field("to_unsigned_request", &"<function>")
            .field("sync_timestamp", &"<function>")
            .field("transport", &self.transport)
            .field("null_signer", &self.null_signer)
            .field("credentials", &"<redacted>")
            .field("create_signer", &self.create_signer)
            .field("authenticate_legs", &self.authenticate_legs)
            .field("signer", &"<redacted>")
            .field("authenticated_epoch", &self.authenticated_epoch)
            .field("auth_gate", &self.auth_gate)
            .finish()
    }
}

/// Serializes authentication so that at most one authentication runs at a
/// time: a caller that finds the session stale while another authentication is
/// in flight waits for it to finish instead of starting a second one.
#[derive(Default)]
struct AuthGate {
    state: Mutex<AuthGateState>,
}

#[derive(Default)]
enum AuthGateState {
    #[default]
    Idle,
    Authenticating(AuthCompleted),
}

enum AuthGateAcquisition {
    /// This caller is now the only task running authentication.
    Authenticator(AuthCompleted),
    /// Another caller is authenticating; wait for its completion signal.
    Waiting(AuthCompleted),
}

impl AuthGate {
    /// Atomically marks the gate as busy, returning the completion signal the
    /// caller must notify when it finishes, or the in-flight signal to wait on
    /// if another authentication is already running.
    fn acquire(&self) -> EGResult<AuthGateAcquisition> {
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
    fn release(&self, completed: &AuthCompleted) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(_) => return,
        };
        if let AuthGateState::Authenticating(active) = &*state {
            if Arc::ptr_eq(&active.0, &completed.0) {
                *state = AuthGateState::Idle;
            }
        }
    }
}

impl std::fmt::Debug for AuthGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthGate").finish_non_exhaustive()
    }
}

/// A one-shot signal that an in-flight authentication has finished, shared
/// between the authenticator and every caller waiting on it.
#[derive(Clone, Default)]
struct AuthCompleted(Arc<Mutex<AuthCompletedState>>);

#[derive(Default)]
struct AuthCompletedState {
    done: bool,
    wakers: Vec<Waker>,
}

impl AuthCompleted {
    fn wait(&self) -> impl Future<Output = EGResult<()>> + Send + '_ {
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
