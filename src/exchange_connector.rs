use crate::{
    destroy::Destroy, exchange_protocol::ExchangeProtocol, session::Session,
    transport::transport::Transport,
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    error::result::StockTrekResult,
    order::{order_request::OrderRequest, order_response::OrderResponse},
};

pub type ExchangeConnector = Box<dyn ExchangeConnectorTrait>;

#[async_trait]
pub trait ExchangeConnectorTrait {
    async fn authenticate(&mut self) -> StockTrekResult<()>;
    async fn send_order(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse>;
}

pub struct ExchangeConnectorImpl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    protocol: ExchangeProtocol<TState, TCredentials, TTransports, TTransport, TMessage, TReply>,
    credentials: TCredentials,
    transports: TTransports,
    session: Session<TState>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    ExchangeConnectorImpl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        protocol: ExchangeProtocol<TState, TCredentials, TTransports, TTransport, TMessage, TReply>,
        credentials: TCredentials,
        transports: TTransports,
    ) -> ExchangeConnector {
        Box::new(Self {
            protocol,
            credentials,
            transports,
            session: Session::new(),
        })
    }
}

#[async_trait]
impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply> ExchangeConnectorTrait
    for ExchangeConnectorImpl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    async fn authenticate(&mut self) -> StockTrekResult<()> {
        self.protocol
            .authenticate(&self.credentials, &self.transports, &mut self.session)
            .await
    }
    async fn send_order(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        self.protocol
            .send_order_request(
                &self.credentials,
                &self.transports,
                &self.session,
                order_request,
            )
            .await
    }
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply> Destroy
    for ExchangeConnectorImpl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    fn destroy(&mut self) {
        self.session.destroy();
    }
}
