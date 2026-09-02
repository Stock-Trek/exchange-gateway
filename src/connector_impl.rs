use crate::{
    clock::{Clock, Synchronization},
    connector::Connector,
    error::{EGError, EGResult},
    functions::ArcPredicate,
    rate_limiter::RateLimiter,
    transports::transport::{Transport, TransportTrait},
};
use async_trait::async_trait;
use exchange_types::rate_limited::RateLimitType;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub struct ConnectorImpl<EGReq, TransportReq, TransportRes, EGRes> {
    rate_limiter: Arc<dyn RateLimiter>,
    clock: Clock,
    synchronization: Synchronization<EGReq, EGRes>,
    to_weight: fn(&EGReq) -> u32,
    to_order_count: fn(&EGReq) -> u32,
    to_filter: fn(&EGReq) -> ArcPredicate<EGRes>,
    transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
}

#[async_trait]
impl<EGReq, TransportReq, TransportRes, EGRes> Connector
    for ConnectorImpl<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    TransportRes: Send,
    TransportReq: Send,
    EGRes: Send + Sync + 'static,
{
    type Request = EGReq;
    type Response = EGRes;

    async fn connect(&self) -> EGResult<()> {
        self.transport.connect().await
    }
    async fn sync_clock(&self) -> EGResult<()> {
        let request = (self.synchronization.create_time_request)();
        let filter = (self.to_filter)(&request);
        self.check_rate_limits(&request)?;
        let start = Instant::now();
        let response = self
            .transport
            .send_and_wait_for(request, self.synchronization.timeout, filter)
            .await?;
        let round_trip_time = start.elapsed();
        let server_time = (self.synchronization.to_server_time)(&response)?;
        self.clock.sync(server_time, round_trip_time)?;
        Ok(())
    }
    fn is_connected(&self) -> EGResult<bool> {
        Ok(self.transport.is_connected())
    }
    async fn send(&self, request: EGReq, timeout: Duration) -> EGResult<EGRes> {
        self.check_rate_limits(&request)?;
        let filter = (self.to_filter)(&request);
        self.transport
            .send_and_wait_for(request, timeout, filter)
            .await
    }
    async fn disconnect(&self) -> EGResult<()> {
        self.transport.disconnect().await
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes>
    ConnectorImpl<EGReq, TransportReq, TransportRes, EGRes>
where
    EGReq: Send,
    EGRes: Send + Sync + 'static,
{
    pub(crate) fn new(
        rate_limiter: Arc<dyn RateLimiter>,
        clock: Clock,
        synchronization: Synchronization<EGReq, EGRes>,
        to_weight: fn(&EGReq) -> u32,
        to_order_count: fn(&EGReq) -> u32,
        to_filter: fn(&EGReq) -> ArcPredicate<EGRes>,
        transport: Transport<EGReq, TransportReq, TransportRes, EGRes>,
    ) -> Self {
        Self {
            rate_limiter,
            clock,
            synchronization,
            to_weight,
            to_order_count,
            to_filter,
            transport,
        }
    }
    fn check_rate_limits(&self, request: &EGReq) -> EGResult<()> {
        let weight = (self.to_weight)(request);
        let order_count = (self.to_order_count)(request);
        let limit_costs = vec![
            (RateLimitType::Weight, weight),
            (RateLimitType::OrderCount, order_count),
        ];
        if !self.rate_limiter.did_acquire(&limit_costs) {
            Err(EGError::RateLimited(limit_costs))
        } else {
            Ok(())
        }
    }
}

impl<EGReq, TransportReq, TransportRes, EGRes> std::fmt::Debug
    for ConnectorImpl<EGReq, TransportReq, TransportRes, EGRes>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connector")
            .field("rate_limiter", &self.rate_limiter)
            .field("clock", &self.clock)
            .field("synchronization", &self.synchronization)
            .field("to_weight", &"<function>")
            .field("to_order_count", &"<function>")
            .field("to_filter", &"<function>")
            .field("transport", &self.transport)
            .finish()
    }
}
