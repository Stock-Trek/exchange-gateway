use crate::{
    cex::rate_limits_weights::RequestWeights,
    error::EGResult,
    exchange_spec::ExchangeSpecTrait,
    functions::{CreateAuthMessage, CreateSignerFrom, TryConvertFromRequest, TryConvertToResponse},
    messenger::Messenger,
    sign::signer::Signer,
};
use async_trait::async_trait;
use chrono::Duration;

pub struct CexSpec<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
{
    #[allow(unused)]
    request_weights: RequestWeights,
    messenger: Messenger<TMessageToExchange, TMessageFromExchange>,
    increments_leg: IncrementsLeg<TMessageToExchange, TMessageFromExchange, TResponse>,
    authenticate_legs:
        Vec<AuthenticateLeg<TUnsignedMessage, TMessageToExchange, TMessageFromExchange>>,
    request_leg: RequestLeg<TRequest, TUnsignedMessage, TMessageFromExchange, TResponse>,
}

pub struct IncrementsLeg<TMessageToExchange, TMessageFromExchange, TResponse> {
    pub(crate) message: TMessageToExchange,
    pub(crate) timeout: Duration,
    pub(crate) to_response: TryConvertToResponse<TMessageFromExchange, TResponse>,
}

pub struct AuthenticateLeg<TUnsignedMessage, TMessageToExchange, TMessageFromExchange> {
    pub(crate) create_auth_message: CreateAuthMessage<TUnsignedMessage>,
    pub(crate) timeout: Duration,
    pub(crate) create_signer_from:
        CreateSignerFrom<TMessageFromExchange, TUnsignedMessage, TMessageToExchange>,
}

pub struct RequestLeg<TRequest, TUnsignedMessage, TMessageFromExchange, TResponse> {
    pub(crate) to_unsigned_message: TryConvertFromRequest<TRequest, TUnsignedMessage>,
    pub(crate) timeout: Duration,
    pub(crate) to_response: TryConvertToResponse<TMessageFromExchange, TResponse>,
}

impl<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
    CexSpec<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
where
    TRequest: Send + Sync + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TMessageToExchange: Send + Sync + 'static,
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub fn new(
        request_weights: RequestWeights,
        messenger: Messenger<TMessageToExchange, TMessageFromExchange>,
        increments_leg: IncrementsLeg<TMessageToExchange, TMessageFromExchange, TResponse>,
        authenticate_legs: Vec<
            AuthenticateLeg<TUnsignedMessage, TMessageToExchange, TMessageFromExchange>,
        >,
        request_leg: RequestLeg<TRequest, TUnsignedMessage, TMessageFromExchange, TResponse>,
    ) -> Self {
        Self {
            request_weights,
            messenger,
            increments_leg,
            authenticate_legs,
            request_leg,
        }
    }
}

#[async_trait]
impl<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
    ExchangeSpecTrait<TRequest, TUnsignedMessage, TMessageToExchange, TResponse>
    for CexSpec<TRequest, TUnsignedMessage, TMessageToExchange, TMessageFromExchange, TResponse>
where
    TRequest: Send + Sync,
    TUnsignedMessage: Send + Sync,
    TMessageToExchange: Send + Sync,
    TMessageFromExchange: Send + Sync,
    TResponse: Send + Sync,
{
    async fn increments(&self) -> EGResult<TResponse> {
        let message_from = self
            .messenger
            .send(&self.increments_leg.message, self.increments_leg.timeout)
            .await?;
        let response = ((self.increments_leg.to_response)(message_from))?;
        Ok(response)
    }
    async fn authenticate(
        &self,
        mut signer: Signer<TUnsignedMessage, TMessageToExchange>,
    ) -> EGResult<Signer<TUnsignedMessage, TMessageToExchange>> {
        for leg in &self.authenticate_legs {
            let auth_message = (leg.create_auth_message)();
            let signed_auth_message = signer.sign(auth_message)?;
            let message_from = self
                .messenger
                .send(&signed_auth_message, leg.timeout)
                .await?;
            signer = (leg.create_signer_from)(&message_from)?;
        }
        Ok(signer)
    }
    async fn send(
        &self,
        request: TRequest,
        signer: &Signer<TUnsignedMessage, TMessageToExchange>,
    ) -> EGResult<TResponse> {
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
        let unsigned = (self.request_leg.to_unsigned_message)(&request)?;
        let message_to = signer.sign(unsigned)?;
        let message_from = self
            .messenger
            .send(&message_to, self.request_leg.timeout)
            .await?;
        let response = (self.request_leg.to_response)(message_from)?;
        Ok(response)
    }
}
