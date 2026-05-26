use crate::adapters::binance::binance_structs::{
    BinanceAuthMessage, BinanceCredentials, BinanceHttpTransport, BinanceState,
};

pub type AuthMessageExtractor<TState, TCredentials, TTransport, TExtracted> =
    Box<dyn AuthMessageExtractorTrait<TState, TCredentials, TTransport, TExtracted>>;

pub trait AuthMessageExtractorTrait<TState, TCredentials, TTransport, TExtracted>:
    Send + Sync
{
    fn extract(
        &self,
        state: &TState,
        credentials: &TCredentials,
        transport: &TTransport,
    ) -> TExtracted;
}

pub type AuthMessageFieldExtractor<TState, TCredentials, TTransport, TValue> =
    fn(&TState, &TCredentials, &TTransport) -> TValue;

pub struct BinanceAuthMessageExtractor {
    id: AuthMessageFieldExtractor<BinanceState, BinanceCredentials, BinanceHttpTransport, String>,
}
impl BinanceAuthMessageExtractor {
    pub fn new(
        id: AuthMessageFieldExtractor<
            BinanceState,
            BinanceCredentials,
            BinanceHttpTransport,
            String,
        >,
    ) -> AuthMessageExtractor<
        BinanceState,
        BinanceCredentials,
        BinanceHttpTransport,
        BinanceAuthMessage,
    > {
        Box::new(Self { id })
    }
}

impl
    AuthMessageExtractorTrait<
        BinanceState,
        BinanceCredentials,
        BinanceHttpTransport,
        BinanceAuthMessage,
    > for BinanceAuthMessageExtractor
{
    fn extract(
        &self,
        state: &BinanceState,
        credentials: &BinanceCredentials,
        transport: &BinanceHttpTransport,
    ) -> BinanceAuthMessage {
        let id = (self.id)(state, credentials, transport);
        BinanceAuthMessage { id }
    }
}
