use crate::{
    authentication_state::Unauthenticated,
    exchange_connector::ExchangeConnector,
    spec_creator::SpecCreatorTrait,
    specs::binance::{BinanceCredentials, BinanceHttpSpecCreator, BinanceState},
};
use stock_trek::cex::{
    asset_id::AssetId, order_request::OrderRequest, order_response::OrderResponse,
};

pub struct ConnectorFactory;

impl ConnectorFactory {
    pub async fn binance_http(
        &self,
        credentials: BinanceCredentials,
    ) -> ExchangeConnector<
        BinanceCredentials,
        BinanceState,
        OrderRequest<AssetId, f64>,
        OrderResponse,
        Unauthenticated,
    > {
        BinanceHttpSpecCreator.create_spec();
        ExchangeConnector::new(BinanceHttpSpecCreator.create_spec(), credentials)
    }
}
