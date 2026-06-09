use crate::{
    authenticate_leg::AuthenticateLeg,
    cex::{precise_orders::PreciseOrders, semantic_checker::SemanticChecker},
    exchange_spec::{ExchangeSpec, ExchangeSpecTrait},
    increment_sizes::IncrementSizes,
    message_leg::MessageLeg,
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    cex::{
        asset_id::AssetId, capability::CexCapability, cex_id::CexId, order_request::OrderRequest,
        order_response::OrderResponse, trading_pair::TradingPair,
    },
    error::result::StockTrekResult,
    preferences::Preferences,
};

pub struct CexSpec<TTransports, TCredentials, TState>
where
    TState: Default,
{
    pub id: CexId,
    pub capabilities: Vec<CexCapability>,
    pub increments: HashMap<TradingPair, IncrementSizes>,
    pub authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
    pub message_leg: MessageLeg<
        TTransports,
        TCredentials,
        TState,
        OrderRequest<AssetId, Decimal>,
        OrderResponse,
    >,
}

impl<TTransports, TCredentials, TState> CexSpec<TTransports, TCredentials, TState>
where
    TTransports: Send + Sync + 'static,
    TCredentials: Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub fn new(
        id: CexId,
        capabilities: Vec<CexCapability>,
        increments: HashMap<TradingPair, IncrementSizes>,
        authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
        message_leg: MessageLeg<
            TTransports,
            TCredentials,
            TState,
            OrderRequest<AssetId, Decimal>,
            OrderResponse,
        >,
    ) -> ExchangeSpec<TTransports, TCredentials, TState, OrderRequest<AssetId, f64>, OrderResponse>
    {
        Box::new(Self {
            id,
            capabilities,
            increments,
            authenticate_legs,
            message_leg,
        })
    }
}

#[async_trait]
impl<TTransports, TCredentials, TState>
    ExchangeSpecTrait<TTransports, TCredentials, TState, OrderRequest<AssetId, f64>, OrderResponse>
    for CexSpec<TTransports, TCredentials, TState>
where
    TTransports: Send + Sync,
    TCredentials: Send + Sync,
    TState: Default + Send + Sync,
{
    async fn authenticate(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
    ) -> StockTrekResult<TState> {
        let mut state = TState::default();
        for authentication_leg in &self.authenticate_legs {
            state = match authentication_leg
                .do_leg(transports, credentials, state)
                .await
            {
                Ok(state) => state,
                Err(e) => return Err(e),
            }
        }
        Ok(state)
    }
    async fn send_trade_request(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &TState,
        preferences: &Preferences,
        trade_request: OrderRequest<AssetId, f64>,
    ) -> StockTrekResult<OrderResponse> {
        let precise_trade_request = PreciseOrders.precise_order_request(
            trade_request,
            &self.increments,
            &preferences.cex.rounding,
        )?;
        SemanticChecker.conversion_will_be_semantically_consistent(
            &precise_trade_request,
            &self.capabilities,
            &preferences.cex,
        )?;
        self.message_leg
            .send_trade_request(transports, credentials, state, &precise_trade_request)
            .await
    }
}
