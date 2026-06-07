use crate::{
    authentication_state::Authenticated,
    exchange_connector::ExchangeConnector,
    exchange_spec::ExchangeSpec,
    exchange_spec_creator::ExchangeSpecCreatorTrait,
    specs::binance::{
        BinanceCredentials, BinanceHttpSpecCreator, BinanceHttpTransports, BinanceState,
    },
};
use stock_trek::error::result::StockTrekResult;

pub struct ConnectorFactory;

impl ConnectorFactory {
    pub async fn binance_http(
        &self,
        transports: BinanceHttpTransports,
        credentials: BinanceCredentials,
    ) -> StockTrekResult<
        ExchangeConnector<BinanceHttpTransports, BinanceCredentials, BinanceState, Authenticated>,
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
        spec: ExchangeSpec<TTransports, TCredentials, TState>,
        transports: TTransports,
        credentials: TCredentials,
    ) -> StockTrekResult<ExchangeConnector<TTransports, TCredentials, TState, Authenticated>>
    where
        TTransports: Send + Sync + 'static,
        TCredentials: Send + Sync + 'static,
        TState: Default + Send + Sync + 'static,
    {
        let connector = ExchangeConnector::new(spec, transports, credentials);
        connector.authenticate().await
    }
}
