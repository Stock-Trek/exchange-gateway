use crate::{
    error::EGResult,
    functions::TryConvertRequestTo,
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::signer::Signer,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;

pub type Connector<TRequest> = Box<dyn ConnectorTrait<TRequest>>;

#[async_trait]
pub trait ConnectorTrait<TRequest> {
    async fn send(&self, request: TRequest) -> EGResult<()>;
}

pub(crate) struct ConnectorImpl<TRequest, TUnsignedMessage, TMessageToExchange, TTransport>
where
    TTransport: TransportTrait,
{
    #[allow(unused)]
    pub rate_limits: RateLimits,
    #[allow(unused)]
    pub request_weights: RequestWeights,
    pub request_to_unsigned: TryConvertRequestTo<TRequest, TUnsignedMessage>,
    pub signer: Signer<TUnsignedMessage, TMessageToExchange>,
    pub message_out_to_dto: TryConvertRequestTo<TMessageToExchange, TTransport::MessageDto>,
    pub transport: TTransport,
    pub timeout: Duration,
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TMessageToExchange, TTransport> ConnectorTrait<TRequest>
    for ConnectorImpl<TRequest, TUnsignedMessage, TMessageToExchange, TTransport>
where
    TRequest: Send,
    TUnsignedMessage: Send,
    TMessageToExchange: Send,
    TTransport: TransportTrait,
{
    async fn send(&self, request: TRequest) -> EGResult<()> {
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
        let message_to = self.signer.sign(unsigned)?;
        let message_dto = (self.message_out_to_dto)(&message_to)?;
        self.transport.send(message_dto, self.timeout).await
    }
}
