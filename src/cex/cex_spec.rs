use crate::{
    authenticate_leg::AuthenticateLeg,
    cex::{
        increment_sizes::IncrementSizes, precise_orders::PreciseOrders,
        rate_limits_weights::RequestWeights, semantic_checker::SemanticChecker,
    },
    exchange_spec::{ExchangeSpec, ExchangeSpecTrait},
    increments_leg::IncrementsLeg,
    message_leg::MessageLeg,
    sign::signer::Signer,
};
use async_trait::async_trait;
use bimap::BiMap;
use rust_decimal::Decimal;
use std::collections::HashMap;
use stock_trek::{
    cex::{
        asset_id::AssetId, capability::CexCapability, order_request::OrderRequest,
        order_response::OrderResponse, trading_pair::TradingPair,
    },
    error::result::StockTrekResult,
    preferences::Preferences,
};

pub struct CexSpec<TUnsignedMessage, TSignedMessage> {
    capabilities: Vec<CexCapability>,
    #[allow(unused)]
    request_weights: RequestWeights,
    tickers: BiMap<AssetId, String>,
    increments_leg: IncrementsLeg,
    authenticate_legs: Vec<AuthenticateLeg<TUnsignedMessage, TSignedMessage>>,
    message_leg:
        MessageLeg<OrderRequest<AssetId, Decimal>, TUnsignedMessage, TSignedMessage, OrderResponse>,
}

impl<TUnsignedMessage, TSignedMessage> CexSpec<TUnsignedMessage, TSignedMessage>
where
    TUnsignedMessage: 'static,
    TSignedMessage: 'static,
{
    pub fn new(
        capabilities: Vec<CexCapability>,
        request_weights: RequestWeights,
        tickers: BiMap<AssetId, String>,
        increments_leg: IncrementsLeg,
        authenticate_legs: Vec<AuthenticateLeg<TUnsignedMessage, TSignedMessage>>,
        message_leg: MessageLeg<
            OrderRequest<AssetId, Decimal>,
            TUnsignedMessage,
            TSignedMessage,
            OrderResponse,
        >,
    ) -> ExchangeSpec<OrderRequest<AssetId, f64>, TUnsignedMessage, TSignedMessage, OrderResponse>
    {
        Box::new(Self {
            capabilities,
            request_weights,
            tickers,
            increments_leg,
            authenticate_legs,
            message_leg,
        })
    }
}

#[async_trait]
impl<TUnsignedMessage, TSignedMessage>
    ExchangeSpecTrait<OrderRequest<AssetId, f64>, TUnsignedMessage, TSignedMessage, OrderResponse>
    for CexSpec<TUnsignedMessage, TSignedMessage>
{
    async fn increments(&self) -> StockTrekResult<HashMap<TradingPair, IncrementSizes>> {
        self.increments_leg.get_increments().await
    }
    async fn authenticate(
        &self,
        initial_auth_leg_signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>> {
        let mut signer = initial_auth_leg_signer;
        for authentication_leg in &self.authenticate_legs {
            signer = authentication_leg.do_leg(signer).await?;
        }
        Ok(signer)
    }
    async fn send_trade_request(
        &self,
        preferences: &Preferences,
        trade_request: OrderRequest<AssetId, f64>,
        increments: &HashMap<TradingPair, IncrementSizes>,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<OrderResponse> {
        // TODO add rate limits back in
        // if !self
        //     .rate_limits
        //     .send_order_request
        //     .did_acquire(self.request_weights.send_order_request)
        // {
        //     return Err(StockTrekError::General(GeneralError::Message(
        //         "Rate limited".to_string(),
        //     )));
        // }
        let precise_trade_request = PreciseOrders.precise_order_request(
            trade_request,
            increments,
            &preferences.cex.rounding,
        )?;
        SemanticChecker.conversion_will_be_semantically_consistent(
            &precise_trade_request,
            &self.capabilities,
            &preferences.cex,
        )?;
        self.message_leg
            .send_trade_request(preferences, &self.tickers, precise_trade_request, signer)
            .await
    }
}
