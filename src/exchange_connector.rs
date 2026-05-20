use crate::{
    auth_spec::AuthSpec, convert::order_marshaller::OrderRequestMarshallUnmarshaller,
    destroy::Destroy, session::Session,
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
        order_request: &OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse>;
}

pub struct ExchangeConnectorGeneric<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Default + Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    auth_spec: AuthSpec<TState, TCredentials, TTransports, TMessage, TReply>,
    credentials: TCredentials,
    transports: TTransports,
    order_request_marshall_unmarshaller: OrderRequestMarshallUnmarshaller<TMessage, TReply>,
    session: Session<TState>,
}

impl<TState, TCredentials, TTransports, TMessage, TReply>
    ExchangeConnectorGeneric<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Default + Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        auth_spec: AuthSpec<TState, TCredentials, TTransports, TMessage, TReply>,
        credentials: TCredentials,
        transports: TTransports,
        order_request_marshall_unmarshaller: OrderRequestMarshallUnmarshaller<TMessage, TReply>,
    ) -> ExchangeConnector {
        Box::new(Self {
            auth_spec,
            credentials,
            transports,
            order_request_marshall_unmarshaller,
            session: Session::new(),
        })
    }
}

#[async_trait]
impl<TState, TCredentials, TTransports, TMessage, TReply> ExchangeConnectorTrait
    for ExchangeConnectorGeneric<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Default + Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    async fn authenticate(&mut self) -> StockTrekResult<()> {
        self.auth_spec
            .authenticate(&self.credentials, &self.transports, &mut self.session)
            .await?;
        Ok(())
    }
    async fn send_order(
        &self,
        order_request: &OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        let mut message = self
            .order_request_marshall_unmarshaller
            .marshall(order_request);
        let reply = self
            .auth_spec
            .sign(
                &self.credentials,
                &self.transports,
                &self.session,
                &mut message,
            )
            .await?;
        let order_response = self.order_request_marshall_unmarshaller.unmarshall(&reply);
        Ok(order_response)
    }
}

impl<TState, TCredentials, TTransports, TMessage, TReply> Destroy
    for ExchangeConnectorGeneric<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Default + Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    fn destroy(&mut self) {
        self.session.destroy();
    }
}
