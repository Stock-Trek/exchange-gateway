use crate::{
    error::{EGError, EGResult},
    functions::{ArcTryConvertRef, ArcTryConvertValue},
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::signer::Signer,
    transports::transport::Transport,
};
use std::time::Duration;

pub struct ConnectorSession<
    TRequest,
    TUnsignedMessageToExchange,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
> where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    #[allow(unused)]
    pub(crate) rate_limits: RateLimits,
    #[allow(unused)]
    pub(crate) request_weights: RequestWeights,
    pub(crate) request_to_unsigned: ArcTryConvertRef<TRequest, TUnsignedMessageToExchange>,
    pub(crate) null_signer: Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) signer: Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) transport: Transport<TMessageToExchange, TMessageFromExchange, TResponse>,
}

impl<TRequest, TUnsignedMessageToExchange, TMessageToExchange, TMessageFromExchange, TResponse>
    ConnectorSession<
        TRequest,
        TUnsignedMessageToExchange,
        TMessageToExchange,
        TMessageFromExchange,
        TResponse,
    >
where
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub async fn fire_and_forget(
        &self,
        request: TRequest,
        signed: bool,
        timeout: Duration,
    ) -> EGResult<()> {
        self.check_rate_limits()?;
        let unsigned = (self.request_to_unsigned)(&request).map_err(EGError::Convert)?;
        let message_to = match signed {
            true => self.signer.sign(unsigned),
            false => self.null_signer.sign(unsigned),
        }?;
        self.transport.fire_and_forget(message_to, timeout).await
    }
    pub async fn send_and_wait_for_response<TWaitedResponse>(
        &self,
        request: TRequest,
        signed: bool,
        timeout: Duration,
        filter_response: ArcTryConvertValue<TResponse, TWaitedResponse>,
    ) -> EGResult<TWaitedResponse>
    where
        TWaitedResponse: Send + Sync + 'static,
    {
        self.check_rate_limits()?;
        let unsigned = (self.request_to_unsigned)(&request).map_err(EGError::Convert)?;
        let message_to = match signed {
            true => self.signer.sign(unsigned),
            false => self.null_signer.sign(unsigned),
        }?;
        self.transport
            .send_and_wait_for_response(message_to, timeout, filter_response)
            .await
    }
    fn check_rate_limits(&self) -> EGResult<()> {
        // TODO add rate limits back in
        // if !self
        //     .rate_limits
        //     .send_order_request
        //     .did_acquire(self.request_weights.send_order_request)
        // {
        //     return Err(EGError::BadResponse);
        // }
        Ok(())
    }
}
