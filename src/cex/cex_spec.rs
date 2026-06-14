use crate::{
    authenticate_leg::AuthenticateLeg,
    cex::{
        increment_sizes::IncrementSizes,
        precise_orders::PreciseOrders,
        rate_limits_weights::{RateLimits, RequestWeights},
        semantic_checker::SemanticChecker,
    },
    credentials::Credential,
    exchange_spec::{ExchangeSpec, ExchangeSpecTrait},
    message_leg::MessageLeg,
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    cex::{
        asset_id::AssetId, capability::CexCapability, order_request::OrderRequest,
        order_response::OrderResponse, trading_pair::TradingPair,
    },
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
    preferences::Preferences,
};

pub struct CexSpec<TTransports, TState>
where
    TState: Default,
{
    capabilities: Vec<CexCapability>,
    increments: HashMap<TradingPair, IncrementSizes>,
    rate_limits: RateLimits,
    request_weights: RequestWeights,
    authenticate_legs: Vec<AuthenticateLeg<TTransports, TState>>,
    message_leg: MessageLeg<TTransports, TState, OrderRequest<AssetId, Decimal>, OrderResponse>,
}

impl<TTransports, TState> CexSpec<TTransports, TState>
where
    TTransports: Send + Sync + 'static,
    TState: Default + Send + Sync + 'static,
{
    pub fn new(
        capabilities: Vec<CexCapability>,
        increments: HashMap<TradingPair, IncrementSizes>,
        rate_limits: RateLimits,
        request_weights: RequestWeights,
        authenticate_legs: Vec<AuthenticateLeg<TTransports, TState>>,
        message_leg: MessageLeg<TTransports, TState, OrderRequest<AssetId, Decimal>, OrderResponse>,
    ) -> ExchangeSpec<TTransports, TState, OrderRequest<AssetId, f64>, OrderResponse> {
        Box::new(Self {
            capabilities,
            increments,
            rate_limits,
            request_weights,
            authenticate_legs,
            message_leg,
        })
    }
}

#[async_trait]
impl<TTransports, TState>
    ExchangeSpecTrait<TTransports, TState, OrderRequest<AssetId, f64>, OrderResponse>
    for CexSpec<TTransports, TState>
where
    TTransports: Send + Sync,
    TState: Default + Send + Sync,
{
    async fn authenticate(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
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
        credentials: &dyn Credential,
        state: &TState,
        preferences: &Preferences,
        trade_request: OrderRequest<AssetId, f64>,
    ) -> StockTrekResult<OrderResponse> {
        if !self
            .rate_limits
            .send_order_request
            .did_acquire(self.request_weights.send_order_request)
        {
            return Err(StockTrekError::General(GeneralError::Message(
                "Rate limited".to_string(),
            )));
        }
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
