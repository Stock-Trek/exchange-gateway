use crate::{
    auth_gate::{AuthGate, AuthGateAcquisition},
    authenticate_leg::AuthenticateLeg,
    clock::{Clock, Synchronization},
    connector::Connector,
    error::{EGError, EGResult},
    functions::{ArcTryConvertValue, TryConvertRef, TryConvertValue},
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
    sign::{
        convert_signer::ConvertSigner,
        signer::{Signer, SignerTrait},
    },
    transports::transport::{Transport, TransportTrait},
};
use async_trait::async_trait;
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
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
    clock: Clock,
    synchronization: Synchronization<EGUnsignedReq, EGRes>,
    to_unsigned_request: TryConvertValue<ExternalReq, EGUnsignedReq>,
    to_weight: fn(&EGUnsignedReq) -> u32,
    to_order_count: fn(&EGUnsignedReq) -> u32,
    sync_timestamp_fields: ArcTryConvertValue<(EGUnsignedReq, i64), EGUnsignedReq>,
    transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
    null_signer: ConvertSigner<EGUnsignedReq, EGReq>,
    credentials: Option<TCredentials>,
    create_signer: TryConvertRef<TCredentials, Signer<EGUnsignedReq, EGReq>>,
    authenticate_legs: Vec<AuthenticateLeg<EGUnsignedReq, EGReq, EGRes>>,
    signer: Arc<Mutex<Option<Signer<EGUnsignedReq, EGReq>>>>,
    auth_gate: Arc<AuthGate>,
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
        self.transport.connect().await
    }
    async fn sync_clock(&self) -> EGResult<()> {
        let (signed_message, weight, order_count, filter) = {
            let (message, filter) = (self.synchronization.create_time_request)();
            let weight = (self.to_weight)(&message);
            let order_count = (self.to_order_count)(&message);
            self.check_rate_limits(&message)?;
            let signed_message = match self.signed_request(message, false) {
                Ok(signed_message) => signed_message,
                Err(error) => {
                    let _ = self.rate_limits.refund(weight, order_count);
                    return Err(error);
                }
            };
            (signed_message, weight, order_count, filter)
        };
        let start = Instant::now();
        let response = match self
            .transport
            .send_and_wait_for(signed_message, self.synchronization.timeout, filter)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let _ = self.rate_limits.refund(weight, order_count);
                return Err(error);
            }
        };
        let round_trip_time = start.elapsed();
        let server_time = (self.synchronization.to_server_time)(&response)?;
        self.clock.sync(server_time, round_trip_time);
        Ok(())
    }
    async fn authenticate(&self) -> EGResult<()> {
        let Some(credentials) = &self.credentials else {
            return Ok(());
        };
        loop {
            if !self.is_authentication_stale()? {
                return Ok(());
            }
            let guard = match self.auth_gate.acquire()? {
                AuthGateAcquisition::Acquired(guard) => guard,
                AuthGateAcquisition::Blocked(on_complete) => {
                    on_complete.wait().await?;
                    continue;
                }
            };
            let result = self.run_authentication(credentials).await;
            if result.is_ok() {
                guard.complete();
            }
            match result {
                Err(error) => return Err(error),
                Ok(()) => {
                    if self.is_authentication_stale()? {
                        continue;
                    }
                    return Ok(());
                }
            }
        }
    }
    fn is_connected(&self) -> EGResult<bool> {
        Ok(self.transport.is_connected())
    }
    fn is_authenticated(&self) -> EGResult<bool> {
        if !self.transport.is_connected() {
            return Ok(false);
        }
        let has_signer = self
            .signer
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .is_some();
        Ok(has_signer && !self.is_authentication_stale()?)
    }
    async fn disconnect(&self) -> EGResult<()> {
        {
            let mut guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            *guard = None;
        }
        self.transport.disconnect().await
    }
    async fn send(&self, request: ExternalReq, signed: bool, timeout: Duration) -> EGResult<()> {
        if signed && self.is_authentication_stale()? {
            return Err(EGError::NotAuthenticated);
        }
        let (signed_request, weight, order_count) = {
            let unsigned = (self.to_unsigned_request)(request)?;
            let unsigned = if signed {
                (self.sync_timestamp_fields)((unsigned, self.clock.now_millis()))?
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
                if matches!(&error, EGError::RateLimited { .. }) {
                    let _ = self.rate_limits.refund(weight, order_count);
                }
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
    pub(crate) fn new(
        rate_limits: RateLimits,
        clock: Clock,
        synchronization: Synchronization<EGUnsignedReq, EGRes>,
        to_unsigned_request: TryConvertValue<ExternalReq, EGUnsignedReq>,
        to_weight: fn(&EGUnsignedReq) -> u32,
        to_order_count: fn(&EGUnsignedReq) -> u32,
        sync_timestamp_fields: ArcTryConvertValue<(EGUnsignedReq, i64), EGUnsignedReq>,
        transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
        null_signer: ConvertSigner<EGUnsignedReq, EGReq>,
        credentials: Option<TCredentials>,
        create_signer: TryConvertRef<TCredentials, Signer<EGUnsignedReq, EGReq>>,
        authenticate_legs: Vec<AuthenticateLeg<EGUnsignedReq, EGReq, EGRes>>,
        auth_gate: Arc<AuthGate>,
    ) -> Self {
        Self {
            rate_limits,
            clock,
            to_weight,
            to_order_count,
            to_unsigned_request,
            sync_timestamp_fields,
            transport,
            null_signer,
            credentials,
            create_signer,
            authenticate_legs,
            synchronization,
            signer: Arc::new(Mutex::new(None)),
            auth_gate,
        }
    }

    async fn run_authentication(&self, credentials: &TCredentials) -> EGResult<()> {
        let mut signer = (self.create_signer)(credentials)?;
        for leg in &self.authenticate_legs {
            let (signed_auth_message, weight, order_count, filter) = {
                let (auth_message, filter) = (leg.create_auth_attempt)(&self.clock);
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
                (signed_auth_message, weight, order_count, filter)
            };
            let authentication_response = match self
                .transport
                .send_and_wait_for(signed_auth_message, leg.timeout, filter)
                .await
            {
                Ok(authentication_response) => authentication_response,
                Err(error) => {
                    let _ = self.rate_limits.refund(weight, order_count);
                    return Err(error);
                }
            };
            signer = match (leg.create_signer)(authentication_response)? {
                Some(next_signer) => next_signer,
                None => signer,
            };
        }
        {
            let mut guard = self.signer.lock().map_err(|_| EGError::MutexPoisoned)?;
            *guard = Some(signer);
        }
        Ok(())
    }
    fn is_authentication_stale(&self) -> EGResult<bool> {
        let has_signer = self
            .signer
            .lock()
            .map_err(|_| EGError::MutexPoisoned)?
            .is_some();
        Ok(!has_signer
            || (!self.authenticate_legs.is_empty()
                && (self.auth_gate.is_stale()? || !self.transport.is_connected())))
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
            return Err(EGError::RateLimited(RateLimitFeedback::default()));
        }
        let order_count = (self.to_order_count)(unsigned);
        if !self.rate_limits.orders.did_acquire(order_count)? {
            let _ = self.rate_limits.weight.refund(weight);
            return Err(EGError::RateLimited(RateLimitFeedback::default()));
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
            .field("clock", &self.clock)
            .field("synchronization", &self.synchronization)
            .field("to_unsigned_request", &"<function>")
            .field("to_weight", &"<function>")
            .field("to_order_count", &"<function>")
            .field("sync_timestamp", &"<function>")
            .field("transport", &self.transport)
            .field("null_signer", &self.null_signer)
            .field("credentials", &"<redacted>")
            .field("create_signer", &self.create_signer)
            .field("authenticate_legs", &self.authenticate_legs)
            .field("signer", &"<redacted>")
            .field("auth_gate", &self.auth_gate)
            .finish()
    }
}
