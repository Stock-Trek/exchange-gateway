use crate::{
    authentication_state::{AuthState, Authenticated, Scratch, Unauthenticated},
    destroy::Destroy,
    exchange_spec::ExchangeSpec,
    precise_orders::PreciseOrders,
    semantic_checker::SemanticChecker,
};
use std::marker::PhantomData;
use stock_trek::{
    asset_id::AssetId,
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
    order::{order_request::OrderRequest, order_response::OrderResponse},
    preferences::Preferences,
};

pub struct ExchangeConnector<TTransports, TCredentials, TDomainState, TAuthState>
where
    TCredentials: Destroy,
    TDomainState: Default,
    TAuthState: AuthState,
{
    spec: ExchangeSpec<TTransports, TCredentials, TDomainState>,
    transports: TTransports,
    credentials: TCredentials,
    state: TDomainState,
    _phantom_auth_state: PhantomData<TAuthState>,
}

impl<TTransports, TCredentials, TDomainState>
    ExchangeConnector<TTransports, TCredentials, TDomainState, Scratch>
where
    TTransports: Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TDomainState: Default + Send + Sync + 'static,
{
    pub fn new(
        spec: ExchangeSpec<TTransports, TCredentials, TDomainState>,
        transports: TTransports,
        credentials: TCredentials,
    ) -> ExchangeConnector<TTransports, TCredentials, TDomainState, Unauthenticated> {
        ExchangeConnector::<TTransports, TCredentials, TDomainState, Unauthenticated> {
            spec,
            transports,
            credentials,
            state: TDomainState::default(),
            _phantom_auth_state: PhantomData,
        }
    }
}

impl<TTransports, TCredentials, TDomainState>
    ExchangeConnector<TTransports, TCredentials, TDomainState, Unauthenticated>
where
    TTransports: Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TDomainState: Default + Send + Sync + 'static,
{
    pub async fn authenticate(
        self,
    ) -> StockTrekResult<ExchangeConnector<TTransports, TCredentials, TDomainState, Authenticated>>
    {
        let mut state = self.state;
        for authentication_leg in &self.spec.authenticate_legs {
            state = match authentication_leg
                .do_leg(&self.transports, &self.credentials, state)
                .await
            {
                Ok(state) => state,
                Err(e) => {
                    self.credentials.destroy();
                    return Err(e);
                }
            }
        }
        Ok(
            ExchangeConnector::<TTransports, TCredentials, TDomainState, Authenticated> {
                spec: self.spec,
                transports: self.transports,
                credentials: self.credentials,
                state,
                _phantom_auth_state: PhantomData,
            },
        )
    }
}

impl<TTransports, TCredentials, TDomainState>
    ExchangeConnector<TTransports, TCredentials, TDomainState, Authenticated>
where
    TTransports: Send + Sync,
    TCredentials: Destroy + Send + Sync,
    TDomainState: Default + Send + Sync,
{
    pub async fn send_order_request(
        &self,
        order_request: OrderRequest<AssetId, f64>,
        preferences: &Preferences,
    ) -> StockTrekResult<OrderResponse> {
        let precise_order_request = PreciseOrders.precise_order_request(
            order_request,
            &self.spec.increments,
            &preferences.rounding,
        )?;
        if !SemanticChecker.conversion_will_be_semantically_consistent(
            &precise_order_request,
            &self.spec.capabilities,
            preferences,
        ) {
            return Err(StockTrekError::General(GeneralError::Message(
                "".to_string(),
            )));
        }
        self.spec
            .message_leg
            .send_order_request(
                &self.transports,
                &self.credentials,
                &self.state,
                precise_order_request,
            )
            .await
    }
}

impl<TTransports, TCredentials, TDomainState, TAuthState> Destroy
    for ExchangeConnector<TTransports, TCredentials, TDomainState, TAuthState>
where
    TCredentials: Destroy,
    TDomainState: Default,
    TAuthState: AuthState,
{
    fn destroy(self) {
        self.credentials.destroy();
    }
}
