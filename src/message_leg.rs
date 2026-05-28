use crate::{destroy::Destroy, transports::transport::TransportTrait};
use async_trait::async_trait;
use chrono::Duration;
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
    order::{order_request::OrderRequest, order_response::OrderResponse},
};

pub type MessageLeg<TTransports, TCredentials, TState> =
    Box<dyn MessageLegTrait<TTransports, TCredentials, TState>>;

#[async_trait]
pub trait MessageLegTrait<TTransports, TCredentials, TState>: Send + Sync {
    async fn send_order_request(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &TState,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse>;
}

pub type GetOrderRequestMessage<TCredentials, TState, TMessage> =
    fn(&TCredentials, &TState, OrderRequest<AssetId, Decimal>) -> StockTrekResult<TMessage>;

pub struct MessageLegImpl<TTransports, TCredentials, TState, TTransport>
where
    TTransport: TransportTrait,
    TCredentials: Destroy,
    TState: Default,
{
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    get_order_request_message: GetOrderRequestMessage<TCredentials, TState, TTransport::MessageDto>,
    get_order_response: fn(TTransport::MessageDto) -> StockTrekResult<OrderResponse>,
}

impl<TTransports, TCredentials, TState, TTransport>
    MessageLegImpl<TTransports, TCredentials, TState, TTransport>
where
    TTransports: Sync + 'static,
    TCredentials: Destroy + Sync + 'static,
    TState: Default + Sync + 'static,
    TTransport: TransportTrait + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
        get_order_request_message: GetOrderRequestMessage<
            TCredentials,
            TState,
            TTransport::MessageDto,
        >,
        get_order_response: fn(TTransport::MessageDto) -> StockTrekResult<OrderResponse>,
    ) -> MessageLeg<TTransports, TCredentials, TState> {
        Box::new(Self {
            get_transport,
            timeout,
            get_order_request_message,
            get_order_response,
        })
    }
}

#[async_trait]
impl<TTransports, TState, TCredentials, TTransport>
    MessageLegTrait<TTransports, TCredentials, TState>
    for MessageLegImpl<TTransports, TCredentials, TState, TTransport>
where
    TTransports: Sync,
    TCredentials: Destroy + Sync,
    TState: Default + Sync,
    TTransport: TransportTrait + 'static,
{
    async fn send_order_request(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &TState,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        let transport = (self.get_transport)(transports);
        let message = (self.get_order_request_message)(credentials, state, order_request)?;
        let reply = transport
            .send(message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.get_order_response)(reply)
    }
}
