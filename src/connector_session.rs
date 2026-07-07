use crate::{
    error::EGResult,
    functions::TryConvertRequestTo,
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::signer::Signer,
    transports::transport::{Transport, TransportMessageDto},
};
use std::time::Duration;

pub struct ConnectorSession<TRequest, TUnsignedMessageToExchange, TMessageToExchange> {
    #[allow(unused)]
    pub(crate) rate_limits: RateLimits,
    #[allow(unused)]
    pub(crate) request_weights: RequestWeights,
    pub(crate) request_to_unsigned: TryConvertRequestTo<TRequest, TUnsignedMessageToExchange>,
    pub(crate) null_signer: Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) signer: Signer<TUnsignedMessageToExchange, TMessageToExchange>,
    pub(crate) message_out_to_dto: TryConvertRequestTo<TMessageToExchange, TransportMessageDto>,
    pub(crate) transport: Transport,
}

impl<TRequest, TUnsignedMessageToExchange, TMessageToExchange>
    ConnectorSession<TRequest, TUnsignedMessageToExchange, TMessageToExchange>
where
    TRequest: Send,
    TUnsignedMessageToExchange: Send,
    TMessageToExchange: Send,
{
    pub async fn request(
        &self,
        request: TRequest,
        signed: bool,
        timeout: Duration,
    ) -> EGResult<()> {
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
        let unsigned = (self.request_to_unsigned)(&request)?;
        let message_to = match signed {
            true => self.signer.sign(unsigned),
            false => self.null_signer.sign(unsigned),
        }?;
        let message_dto = (self.message_out_to_dto)(&message_to)?;
        self.transport.send(message_dto, timeout).await
    }
}
