use crate::{destroy::Destroy, exchange_protocol::ExchangeProtocol, session::Session};
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
    async fn send_order_request(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse>;
}

pub struct ExchangeConnectorImpl<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    protocol: ExchangeProtocol<TTransports, TCredentials, TState>,
    transports: TTransports,
    credentials: TCredentials,
    session: Session<TState>,
}

impl<TTransports, TCredentials, TState> ExchangeConnectorImpl<TTransports, TCredentials, TState>
where
    TTransports: Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub fn new(
        protocol: ExchangeProtocol<TTransports, TCredentials, TState>,
        transports: TTransports,
        credentials: TCredentials,
    ) -> ExchangeConnector {
        Box::new(Self {
            protocol,
            transports,
            credentials,
            session: Session::new(),
        })
    }
}

#[async_trait]
impl<TTransports, TCredentials, TState> ExchangeConnectorTrait
    for ExchangeConnectorImpl<TTransports, TCredentials, TState>
where
    TTransports: Send + Sync,
    TCredentials: Destroy + Send + Sync,
    TState: Default + Send + Sync,
{
    async fn authenticate(&mut self) -> StockTrekResult<()> {
        self.protocol
            .authenticate(&self.transports, &self.credentials, &mut self.session)
            .await
    }
    async fn send_order_request(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        self.protocol
            .send_order_request(
                &self.transports,
                &self.credentials,
                &self.session,
                order_request,
            )
            .await
    }
}

impl<TTransports, TCredentials, TState> Destroy
    for ExchangeConnectorImpl<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    fn destroy(&mut self) {
        self.session.destroy();
    }
}
