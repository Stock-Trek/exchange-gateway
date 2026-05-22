use crate::{
    adapt::{adapter::Adapter, adapter_creator::AdapterCreatorTrait},
    adapters::binance::{
        BinanceCredentials,
        BinanceHttpAdapterCreator,
        BinanceHttpTransports,
        // BinanceWebsocketAdapterCreator, BinanceWebsocketTransports,
    },
};

pub struct AdapterFactory;

impl AdapterFactory {
    pub fn binance_rest(
        credentials: BinanceCredentials,
        transports: BinanceHttpTransports,
    ) -> Adapter {
        BinanceHttpAdapterCreator.create_adapter(credentials, transports)
    }
    // TODO
    // pub fn binance_websocket(
    //     credentials: BinanceCredentials,
    //     transports: BinanceWebsocketTransports,
    // ) -> Adapter
    // {
    //     BinanceWebsocketAdapterCreator.create_adapter(credentials, transports)
    // }
}
