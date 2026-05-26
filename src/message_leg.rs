use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{order_response_extractor::OrderResponseExtractor, signer::Signer},
};
use chrono::Duration;
use rust_decimal::Decimal;
use std::marker::PhantomData;
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
    order_request_extractor: Signer<TState, TCredentials, OrderRequest<AssetId, Decimal>, TMessage>,
    order_response_extractor: OrderResponseExtractor<TReply>,
    _phantom_state: PhantomData<TState>,
    _phantom_credentials: PhantomData<TCredentials>,
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
        order_request_extractor: Signer<
            TState,
            TCredentials,
            OrderRequest<AssetId, Decimal>,
            TMessage,
        >,
        order_response_extractor: OrderResponseExtractor<TReply>,
    ) -> Self {
        Self {
            get_transport,
            timeout,
            order_request_extractor,
            order_response_extractor,
            _phantom_state: PhantomData,
            _phantom_credentials: PhantomData,
        }
    }
    pub async fn send_order_request(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        state: &TState,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        let transport = (self.get_transport)(&transports);
        let message = self
            .order_request_extractor
            .sign(state, credentials, &order_request)?;
        let reply = transport
            .send_and_wait_for_reply(&message, self.timeout)
            .await?;
        let response = self.order_response_extractor.extract(&reply);
        Ok(response)
    }
}
