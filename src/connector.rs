use crate::{
    error::EGResult,
    functions::{TryConvertFromRequest, TryConvertToResponse},
    messenger::Messenger,
    rate_limit::multi_rate_limiter::MultiRateLimiter,
    sign::signer::Signer,
};
use chrono::Duration;

pub struct Connector<
    TRequest,
    TUnsignedMessage,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
> {
    #[allow(unused)]
    pub(crate) request_weights: RequestWeights,
    #[allow(unused)]
    pub(crate) rate_limits: RateLimits,
    pub(crate) to_unsigned_message: TryConvertFromRequest<TRequest, TUnsignedMessage>,
    pub(crate) signer: Signer<TUnsignedMessage, TMessageToExchange>,
    pub(crate) messenger: Messenger<TMessageToExchange, TMessageFromExchange>,
    pub(crate) timeout: Duration,
    pub(crate) to_response: TryConvertToResponse<TMessageFromExchange, TResponse>,
}

impl<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
    Connector<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
{
    pub async fn send(&self, request: TRequest) -> EGResult<TResponse> {
        // TODO add rate limits back in
        // if !self
        //     .rate_limits
        //     .send_order_request
        //     .did_acquire(self.request_weights.send_order_request)
        // {
        //     return Err(EGError::Custom(
        //         "Rate limited".to_string(),
        //     ));
        // }
        let unsigned = (self.to_unsigned_message)(&request)?;
        let message_to = self.signer.sign(unsigned)?;
        let message_from = self.messenger.send(&message_to, self.timeout).await?;
        let response = (self.to_response)(message_from)?;
        Ok(response)
    }
}

#[derive(Debug, Clone)]
pub struct RequestWeights {
    pub send_order_request: u32,
}

#[derive(Debug, Clone)]
pub struct RateLimits {
    pub send_order_request: MultiRateLimiter,
}
