use crate::{
    authenticator_creator::AuthenticatorCreatorTrait,
    connector::Authenticator,
    credentials::{api_key_credential::ApiKeyCredentials, jwt_credential::JwtCredentials},
    specs::{
        binance_websocket::BinanceWebsocketSpecCreator, coinbase_rest::CoinbaseRestSpecCreator,
    },
    transports::{
        http_transport::HttpTransportTrait, websocket_transport::WebsocketTransportTrait,
    },
};
use std::sync::Arc;
use stock_trek::{
    cex::{asset_id::AssetId, order_request::OrderRequest, order_response::OrderResponse},
    error::result::StockTrekResult,
};

pub struct Connectors;

impl Connectors {
    pub async fn binance_websocket<TTransport>(
        &self,
        credentials: ApiKeyCredentials,
        transport: TTransport,
        use_session: bool,
    ) -> StockTrekResult<Authenticator<OrderRequest<AssetId, f64>, OrderResponse>>
    where
        TTransport: WebsocketTransportTrait + 'static,
    {
        let spec_creator = BinanceWebsocketSpecCreator {
            credentials,
            transport: Arc::new(transport),
            use_session,
        };
        spec_creator.into_authenticator()
    }

    pub async fn coinbase_rest<TTransport>(
        &self,
        credentials: JwtCredentials,
        transport: TTransport,
    ) -> StockTrekResult<Authenticator<OrderRequest<AssetId, f64>, OrderResponse>>
    where
        TTransport: HttpTransportTrait + 'static,
    {
        let spec_creator = CoinbaseRestSpecCreator {
            credentials,
            transport: Arc::new(transport),
        };
        spec_creator.into_authenticator()
    }
}
