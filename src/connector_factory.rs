use crate::{
    authentication_state::Unauthenticated,
    cex::cex_spec::CexSpec,
    exchange_connector::ExchangeConnector,
    spec_creator::SpecCreatorTrait,
    specs::binance::{BinanceCredentials, BinanceHttpSpecCreator, BinanceHttpTransports},
};

pub struct ConnectorFactory;

impl ConnectorFactory {
    pub fn binance_http(
        &self,
        transports: BinanceHttpTransports,
        credentials: BinanceCredentials,
    ) -> ExchangeConnector<CexSpec<BinanceHttpSpecCreator>, Unauthenticated> {
        ExchangeConnector::new(
            Box::new(BinanceHttpSpecCreator.create_spec()),
            transports,
            credentials,
        )
    }
}
