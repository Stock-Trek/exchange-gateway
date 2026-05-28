use crate::{authenticate_leg::AuthenticateLeg, destroy::Destroy, message_leg::MessageLeg};
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    error::result::StockTrekResult,
    order::{order_request::OrderRequest, order_response::OrderResponse},
};

pub struct ExchangeProtocol<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
    message_leg: MessageLeg<TTransports, TCredentials, TState>,
}

impl<TTransports, TCredentials, TState> ExchangeProtocol<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    pub fn new(
        authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
        message_leg: MessageLeg<TTransports, TCredentials, TState>,
    ) -> Self {
        Self {
            authenticate_legs,
            message_leg,
        }
    }
    pub async fn authenticate(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &mut TState,
    ) -> StockTrekResult<()> {
        for authenticate_leg in &self.authenticate_legs {
            authenticate_leg
                .do_leg(transports, credentials, state)
                .await?;
        }
        Ok(())
    }
    pub async fn send_order_request(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &TState,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        let response = self
            .message_leg
            .send_order_request(transports, credentials, state, order_request)
            .await?;
        Ok(response)
    }
}
