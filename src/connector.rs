use crate::{
    converter::Converter,
    error::EGResult,
    rate_limit::{rate_limits::RateLimits, request_weights::RequestWeights},
    sign::signer::Signer,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;

pub type Connector<TRequest, TResponse> = Box<dyn ConnectorTrait<TRequest, TResponse>>;

#[async_trait]
pub trait ConnectorTrait<TRequest, TResponse> {
    async fn send(&self, request: TRequest) -> EGResult<()>;
}

pub struct ConnectorImpl<
    TRequest,
    TUnsignedMessage,
    TMessageToExchange,
    TTransport,
    TMessageFromExchange,
    TResponse,
> where
    TTransport: TransportTrait,
{
    #[allow(unused)]
    pub(crate) rate_limits: RateLimits,
    #[allow(unused)]
    pub(crate) request_weights: RequestWeights,
    pub(crate) exchange_converter:
        Converter<TRequest, TUnsignedMessage, TMessageFromExchange, TResponse>,
    pub(crate) signer: Signer<TUnsignedMessage, TMessageToExchange>,
    pub(crate) dto_converter: Converter<
        TMessageToExchange,
        TTransport::MessageDto,
        TTransport::MessageDto,
        TMessageFromExchange,
    >,
    pub(crate) transport: TTransport,
    pub(crate) timeout: Duration,
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TMessageToExchange, TTransport, TMessageFromExchange, TResponse>
    ConnectorTrait<TRequest, TResponse>
    for ConnectorImpl<
        TRequest,
        TUnsignedMessage,
        TMessageToExchange,
        TTransport,
        TMessageFromExchange,
        TResponse,
    >
where
    TRequest: Send,
    TUnsignedMessage: Send,
    TMessageToExchange: Send,
    TTransport: TransportTrait,
    TMessageFromExchange: Send,
    TResponse: Send,
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
        let unsigned = (self.exchange_converter.convert_req(&request))?;
        let message_to = self.signer.sign(unsigned)?;
        let message_dto = self.dto_converter.convert_req(&message_to)?;
        self.transport.send(message_dto, self.timeout).await
    }
}
