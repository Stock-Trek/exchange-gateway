use crate::{
    authentication_state::Unauthenticated,
    exchange_connector::ExchangeConnector,
    spec_creator::SpecCreatorTrait,
    specs::binance::{
        BinanceCredentials, BinanceHttpSpecCreator, BinanceHttpTransports, BinanceState,
    },
};
use stock_trek::cex::{
    asset_id::AssetId, order_request::OrderRequest, order_response::OrderResponse,
};

pub struct ConnectorFactory;

impl ConnectorFactory {
    pub async fn binance_http(
        &self,
        transports: BinanceHttpTransports,
        credentials: BinanceCredentials,
    ) -> ExchangeConnector<
        BinanceHttpTransports,
        BinanceState,
        OrderRequest<AssetId, f64>,
        OrderResponse,
        Unauthenticated,
    > {
        ExchangeConnector::new(
            BinanceHttpSpecCreator.create_spec(),
            transports,
            Box::new(credentials),
        )
    }
}
