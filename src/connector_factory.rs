use crate::{
    authentication_state::Authenticated,
    exchange_connector::ExchangeConnector,
    exchange_spec::ExchangeSpec,
    spec_creator::SpecCreatorTrait,
    specs::binance::{
        BinanceCredentials, BinanceHttpSpecCreator, BinanceHttpTransports, BinanceState,
    },
};
use stock_trek::{
    cex::{asset_id::AssetId, order_request::OrderRequest, order_response::OrderResponse},
    error::result::StockTrekResult,
};

pub struct ConnectorFactory;

impl ConnectorFactory {
    pub async fn binance_http(
        &self,
        transports: BinanceHttpTransports,
        credentials: BinanceCredentials,
    ) -> StockTrekResult<
        ExchangeConnector<
            BinanceHttpTransports,
            BinanceCredentials,
            BinanceState,
            OrderRequest<AssetId, f64>,
            OrderResponse,
            Authenticated,
        >,
    > {
        self.to_authenticated_connector(
            BinanceHttpSpecCreator.create_spec(),
            transports,
            credentials,
        )
        .await
    }
    async fn to_authenticated_connector<TTransports, TCredentials, TState>(
        &self,
        spec: ExchangeSpec<
            TTransports,
            TCredentials,
            TState,
            OrderRequest<AssetId, f64>,
            OrderResponse,
        >,
        transports: TTransports,
        credentials: TCredentials,
    ) -> StockTrekResult<
        ExchangeConnector<
            TTransports,
            TCredentials,
            TState,
            OrderRequest<AssetId, f64>,
            OrderResponse,
            Authenticated,
        >,
    >
    where
        TTransports: Send + Sync + 'static,
        TCredentials: Send + Sync + 'static,
        TState: Default + Send + Sync + 'static,
    {
        let connector = ExchangeConnector::new(spec, transports, credentials);
        connector.authenticate().await
    }
}
