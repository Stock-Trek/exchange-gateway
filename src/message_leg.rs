use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{
        order_message_extractor::OrderMessageExtractor, order_message_signer::OrderMessageSigner,
        order_response_extractor::OrderResponseExtractor,
    },
};
use chrono::Duration;
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    error::result::StockTrekResult,
    order::{order_request::OrderRequest, order_response::OrderResponse},
};

pub struct MessageLeg<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    order_message_extractor: OrderMessageExtractor<TMessage>,
    order_message_signers: Vec<OrderMessageSigner<TState, TCredentials, TMessage>>,
    order_response_extractor: OrderResponseExtractor<TReply>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    MessageLeg<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
        order_message_extractor: OrderMessageExtractor<TMessage>,
        order_response_extractor: OrderResponseExtractor<TReply>,
    ) -> Self {
        Self {
            get_transport,
            timeout,
            order_message_extractor,
            order_message_signers: Vec::new(),
            order_response_extractor,
        }
    }
    pub fn add_order_message_signer(
        mut self,
        signer: OrderMessageSigner<TState, TCredentials, TMessage>,
    ) -> Self {
        self.order_message_signers.push(signer);
        self
    }
    pub async fn send_order_request(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        state: &TState,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        let transport = (self.get_transport)(&transports);
        let mut message = self.order_message_extractor.extract(&order_request);
        for signer in &self.order_message_signers {
            signer.sign(state, credentials, &mut message)?;
        }
        let reply = transport
            .send_and_wait_for_reply(&message, self.timeout)
            .await?;
        let response = self.order_response_extractor.extract(&reply);
        Ok(response)
    }
}
