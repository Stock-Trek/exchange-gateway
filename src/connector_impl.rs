use crate::{
    clock::{Clock, Synchronization},
    connector::Connector,
    error::{EGError, EGResult},
    functions::ArcPredicate,
    rate_limit::{feedback::RateLimitFeedback, rate_limits::RateLimits},
    transports::transport::{Transport, TransportTrait},
};
use async_trait::async_trait;
use std::time::{Duration, Instant};

pub struct ConnectorImpl<Request, TransportReq, TransportRes, Response> {
    rate_limits: RateLimits,
    clock: Clock,
    synchronization: Synchronization<Request, Response>,
    to_weight: fn(&Request) -> u32,
    to_order_count: fn(&Request) -> u32,
    to_filter: fn(&Request) -> ArcPredicate<Response>,
    transport: Transport<Request, TransportReq, TransportRes, Response>,
}

impl<Request, TransportReq, TransportRes, Response>
    ConnectorImpl<Request, TransportReq, TransportRes, Response>
{
    pub(crate) fn new(
        rate_limits: RateLimits,
        synchronization: Synchronization<Request, Response>,
        to_weight: fn(&Request) -> u32,
        to_order_count: fn(&Request) -> u32,
        to_filter: fn(&Request) -> ArcPredicate<Response>,
        transport: Transport<Request, TransportReq, TransportRes, Response>,
    ) -> ConnectorImpl<Request, TransportReq, TransportRes, Response> {
        ConnectorImpl {
            rate_limits,
            clock: Clock::new(),
            synchronization,
            to_weight,
            to_order_count,
            to_filter,
            transport,
        }
    }
    fn check_rate_limits(&self, weight: u32, order_count: u32) -> EGResult<()> {
        if !self.rate_limits.weight.did_acquire(weight)? {
            return Err(EGError::RateLimited(RateLimitFeedback::default()));
        }
        if !self.rate_limits.orders.did_acquire(order_count)? {
            let _ = self.rate_limits.weight.refund(weight);
            return Err(EGError::RateLimited(RateLimitFeedback::default()));
        }
        Ok(())
    }
}

#[async_trait]
impl<Request, TransportReq, TransportRes, Response> Connector<Request, Response>
    for ConnectorImpl<Request, TransportReq, TransportRes, Response>
where
    Request: Send,
    TransportReq: Send,
    TransportRes: Send,
    Response: Send + Sync + 'static,
{
    async fn connect(&self) -> EGResult<()> {
        self.transport.connect().await
    }

    async fn disconnect(&self) -> EGResult<()> {
        self.transport.disconnect().await
    }

    fn server_time_millis(&self) -> EGResult<i64> {
        Ok(self.clock.now_millis())
    }

    async fn sync_clock(&self) -> EGResult<()> {
        let message = (self.synchronization.create_time_request)();
        let weight = (self.to_weight)(&message);
        let order_count = (self.to_order_count)(&message);
        let filter = (self.to_filter)(&message);
        self.check_rate_limits(weight, order_count)?;
        let start = Instant::now();
        let response = match self
            .transport
            .send_and_wait_for(message, self.synchronization.timeout, filter)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if matches!(&error, EGError::RateLimited(..) | EGError::NotSent(..)) {
                    let _ = self.rate_limits.refund(weight, order_count);
                }
                return Err(error);
            }
        };
        let round_trip_time = start.elapsed();
        let server_time = (self.synchronization.to_server_time)(&response)?;
        self.clock.sync(server_time, round_trip_time)
    }

    fn is_connected(&self) -> EGResult<bool> {
        Ok(self.transport.is_connected())
    }

    async fn send(&self, request: Request, timeout: Duration) -> EGResult<Response> {
        let weight = (self.to_weight)(&request);
        let order_count = (self.to_order_count)(&request);
        let filter = (self.to_filter)(&request);
        self.check_rate_limits(weight, order_count)?;
        let response = match self
            .transport
            .send_and_wait_for(request, timeout, filter)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if matches!(&error, EGError::RateLimited(..) | EGError::NotSent(..)) {
                    let _ = self.rate_limits.refund(weight, order_count);
                }
                return Err(error);
            }
        };
        Ok(response)
    }
}

impl<Request, TransportReq, TransportRes, Response> std::fmt::Debug
    for ConnectorImpl<Request, TransportReq, TransportRes, Response>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorImpl")
            .field("rate_limits", &self.rate_limits)
            .field("clock", &self.clock)
            .field("synchronization", &self.synchronization)
            .field("to_weight", &"<function>")
            .field("to_order_count", &"<function>")
            .field("to_filter", &"<function>")
            .field("transport", &self.transport)
            .finish()
    }
}
