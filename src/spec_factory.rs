use crate::{
    credentials::api_key_credential::ApiKeyCredentials,
    exchange_spec::ExchangeSpec,
    spec_creator::SpecCreatorTrait,
    specs::binance_websocket::{
        BinanceWebsocketSpecCreator, SignedMessageToBinance, UnsignedMessageToBinance,
    },
    transports::websocket_transport::WebsocketTransportTrait,
};
use std::sync::Arc;
use stock_trek::{
    cex::{asset_id::AssetId, order_request::OrderRequest, order_response::OrderResponse},
    error::result::StockTrekResult,
};

pub struct SpecFactory;

impl SpecFactory {
    pub async fn binance_websocket(
        &self,
        transport: Arc<dyn WebsocketTransportTrait>,
        credentials: ApiKeyCredentials,
        use_session: bool,
    ) -> StockTrekResult<
        ExchangeSpec<
            OrderRequest<AssetId, f64>,
            UnsignedMessageToBinance,
            SignedMessageToBinance,
            OrderResponse,
        >,
    > {
        BinanceWebsocketSpecCreator {
            credentials,
            transport,
            use_session,
        }
        .into_spec()
    }
}
