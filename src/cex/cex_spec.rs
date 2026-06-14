use crate::{
    authenticate_leg::AuthenticateLeg,
    cex::{
        increment_sizes::IncrementSizes,
        precise_orders::PreciseOrders,
        rate_limits_weights::{RateLimits, RequestWeights},
        semantic_checker::SemanticChecker,
    },
    exchange_spec::ExchangeSpecTrait,
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

pub trait CexSpecTrait:
    ExchangeSpecTrait<TradeRequest = OrderRequest<AssetId, Decimal>, TradeResponse = OrderResponse>
{
}

#[allow(clippy::type_complexity)]
pub struct CexSpec<TSpec: CexSpecTrait + ?Sized> {
    capabilities: Vec<CexCapability>,
    increments: HashMap<TradingPair, IncrementSizes>,
    rate_limits: RateLimits,
    request_weights: RequestWeights,
    authenticate_legs: Vec<AuthenticateLeg<TSpec>>,
    message_leg: MessageLeg<TSpec>,
}

#[allow(clippy::type_complexity)]
impl<TSpec> CexSpec<TSpec>
where
    TSpec: CexSpecTrait + 'static,
{
    pub fn new(
        capabilities: Vec<CexCapability>,
        increments: HashMap<TradingPair, IncrementSizes>,
        rate_limits: RateLimits,
        request_weights: RequestWeights,
        authenticate_legs: Vec<AuthenticateLeg<TSpec>>,
        message_leg: MessageLeg<TSpec>,
    ) -> Self {
        Self {
            capabilities,
            increments,
            rate_limits,
            request_weights,
            authenticate_legs,
            message_leg,
        }
    }
}

#[async_trait]
impl<TSpec> ExchangeSpecTrait for CexSpec<TSpec>
where
    TSpec: CexSpecTrait + 'static,
{
    type Transports = TSpec::Transports;
    type Credentials = TSpec::Credentials;
    type State = TSpec::State;
    type TradeRequest = OrderRequest<AssetId, f64>;
    type TradeResponse = OrderResponse;

    async fn authenticate(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
    ) -> StockTrekResult<Self::State> {
        let mut state = TSpec::State::default();
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
        transports: &Self::Transports,
        credentials: &Self::Credentials,
        state: &Self::State,
        preferences: &Preferences,
        trade_request: Self::TradeRequest,
    ) -> StockTrekResult<Self::TradeResponse> {
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
