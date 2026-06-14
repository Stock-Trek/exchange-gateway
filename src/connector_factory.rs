use crate::{
    authentication_state::Unauthenticated,
    exchange_connector::ExchangeConnector,
    spec_creator::SpecCreatorTrait,
    specs::binance::{
        BinanceCredentials, BinanceHttpSpecCreator, BinanceHttpTransports, BinanceState,
    },
};

use crate::cex::cex_spec::CexSpec;

pub struct ConnectorFactory;

impl ConnectorFactory {
    pub fn binance_http(
        &self,
        transports: BinanceHttpTransports,
        credentials: BinanceCredentials,
    ) -> ExchangeConnector<
        CexSpec<BinanceHttpTransports, BinanceCredentials, BinanceState>,
        Unauthenticated,
    > {
        ExchangeConnector::new(
            Box::new(BinanceHttpSpecCreator.create_spec()),
            transports,
            credentials,
        )
    }
}
