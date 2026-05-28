use crate::{
    authentication_state::{AuthState, Authenticated, Unauthenticated},
    destroy::Destroy,
    exchange_protocol::ExchangeProtocol,
};
use async_trait::async_trait;
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
    order::{order_request::OrderRequest, order_response::OrderResponse},
};

/// Type alias for a boxed trait object that erases the authentication state.
/// Used by Adapter which cannot be generic over the auth state.
pub type ExchangeConnector = Box<dyn ExchangeConnectorTrait>;

#[async_trait]
pub trait ExchangeConnectorTrait: Send + Sync {
    async fn authenticate(&mut self) -> StockTrekResult<()>;
    async fn send_order_request(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse>;
}

/// Main exchange connector using the type-state pattern.
///
/// The generic parameter `TAuthState` tracks the current authentication phase at
/// compile time. Only state-appropriate methods are exposed:
/// - `new()` returns `ExchangeConnectorImpl<Unauthenticated>`
/// - `authenticate()` is only available when `TAuthState = Unauthenticated`
/// - `send_order_request()` is only available when `TAuthState = Authenticated`
pub struct ExchangeConnectorImpl<TTransports, TCredentials, TDomainState, TAuthState>
where
    TCredentials: Destroy,
    TDomainState: Default,
    TAuthState: AuthState,
{
    protocol: ExchangeProtocol<TTransports, TCredentials, TDomainState>,
    transports: TTransports,
    credentials: TCredentials,
    state: TDomainState,
    #[allow(dead_code)]
    auth_state: TAuthState,
}

// ── Construction ────────────────────────────────────────────────────────────

impl<TTransports, TCredentials, TDomainState>
    ExchangeConnectorImpl<TTransports, TCredentials, TDomainState, Unauthenticated>
where
    TTransports: Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TDomainState: Default + Send + Sync + 'static,
{
    /// Create a new connector in the `Unauthenticated` state.
    pub fn new(
        protocol: ExchangeProtocol<TTransports, TCredentials, TDomainState>,
        transports: TTransports,
        credentials: TCredentials,
    ) -> Self {
        Self {
            protocol,
            transports,
            credentials,
            state: TDomainState::default(),
            auth_state: Unauthenticated,
        }
    }

    /// Authenticate by consuming `Self` and returning an `Authenticated` instance.
    pub async fn authenticate(
        mut self,
    ) -> ExchangeConnectorImpl<TTransports, TCredentials, TDomainState, Authenticated> {
        let _ = self
            .protocol
            .authenticate(&self.transports, &self.credentials, &mut self.state)
            .await;
        ExchangeConnectorImpl {
            protocol: self.protocol,
            transports: self.transports,
            credentials: self.credentials,
            state: self.state,
            auth_state: Authenticated,
        }
    }
}

// ── Authenticated operations ───────────────────────────────────────────────

impl<TTransports, TCredentials, TDomainState>
    ExchangeConnectorImpl<TTransports, TCredentials, TDomainState, Authenticated>
where
    TTransports: Send + Sync,
    TCredentials: Destroy + Send + Sync,
    TDomainState: Default + Send + Sync,
{
    /// Send an order request. Only available when authenticated.
    pub async fn send_order_request(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        self.protocol
            .send_order_request(
                &self.transports,
                &self.credentials,
                &self.state,
                order_request,
            )
            .await
    }
}

// ── Erased trait-object bridge ─────────────────────────────────────────────

/// Wrapper that erases the auth-state generic so we can use
/// `ExchangeConnector` (= `Box<dyn ExchangeConnectorTrait>`) in `Adapter`.
///
/// Internally it holds an `Option<ExchangeConnectorImpl<..., Authenticated>>`.
/// The first call to `authenticate()` transitions the inner connector from
/// `Unauthenticated` to `Authenticated`.
pub struct BoxedConnector<TTransports, TCredentials, TDomainState>
where
    TCredentials: Destroy,
    TDomainState: Default,
{
    protocol: ExchangeProtocol<TTransports, TCredentials, TDomainState>,
    transports: TTransports,
    credentials: TCredentials,
    state: TDomainState,
    authenticated: bool,
}

impl<TTransports, TCredentials, TDomainState>
    BoxedConnector<TTransports, TCredentials, TDomainState>
where
    TTransports: Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TDomainState: Default + Send + Sync + 'static,
{
    /// Wrap an unauthenticated connector into a boxable form.
    pub fn from_unauthenticated(
        connector: ExchangeConnectorImpl<TTransports, TCredentials, TDomainState, Unauthenticated>,
    ) -> Self {
        Self {
            protocol: connector.protocol,
            transports: connector.transports,
            credentials: connector.credentials,
            state: connector.state,
            authenticated: false,
        }
    }
}

#[async_trait]
impl<TTransports, TCredentials, TDomainState> ExchangeConnectorTrait
    for BoxedConnector<TTransports, TCredentials, TDomainState>
where
    TTransports: Send + Sync,
    TCredentials: Destroy + Send + Sync,
    TDomainState: Default + Send + Sync,
{
    async fn authenticate(&mut self) -> StockTrekResult<()> {
        if !self.authenticated {
            self.protocol
                .authenticate(&self.transports, &self.credentials, &mut self.state)
                .await?;
            self.authenticated = true;
        }
        Ok(())
    }

    async fn send_order_request(
        &self,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        if !self.authenticated {
            return Err(StockTrekError::General(GeneralError::Message(
                "Cannot send order request before authentication".to_string(),
            )));
        }
        self.protocol
            .send_order_request(
                &self.transports,
                &self.credentials,
                &self.state,
                order_request,
            )
            .await
    }
}

// ── Destroy ────────────────────────────────────────────────────────────────

impl<TTransports, TCredentials, TDomainState, TAuthState> Destroy
    for ExchangeConnectorImpl<TTransports, TCredentials, TDomainState, TAuthState>
where
    TCredentials: Destroy,
    TDomainState: Default,
    TAuthState: AuthState,
{
    fn destroy(&mut self) {
        self.credentials.destroy();
    }
}
