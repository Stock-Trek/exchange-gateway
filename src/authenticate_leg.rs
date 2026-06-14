use crate::transports::transport::TransportTrait;
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub type AuthenticateLeg<TCredentials, TState> =
    Box<dyn AuthenticateLegTrait<TCredentials, TState>>;

#[async_trait]
pub trait AuthenticateLegTrait<TCredentials, TState>: Send + Sync
where
    TState: Default,
{
    async fn do_leg(&self, credentials: &TCredentials, state: TState) -> StockTrekResult<TState>;
}

pub struct AuthenticateLegImpl<TCredentials, TState, TAuthTransport>
where
    TAuthTransport: TransportTrait,
{
    transport: TAuthTransport,
    timeout: Duration,
    get_auth_message: fn(&TAuthTransport, &TCredentials, &TState) -> TAuthTransport::MessageDto,
    update_state: fn(TAuthTransport::MessageDto, state: TState) -> StockTrekResult<TState>,
}

impl<TCredentials, TState, TAuthTransport> AuthenticateLegImpl<TCredentials, TState, TAuthTransport>
where
    TCredentials: Sync + 'static,
    TState: Default + Send + 'static,
    TAuthTransport: TransportTrait + Send + Sync + 'static,
{
    pub fn new(
        transport: TAuthTransport,
        timeout: Duration,
        get_auth_message: fn(&TAuthTransport, &TCredentials, &TState) -> TAuthTransport::MessageDto,
        update_state: fn(TAuthTransport::MessageDto, state: TState) -> StockTrekResult<TState>,
    ) -> AuthenticateLeg<TCredentials, TState> {
        Box::new(Self {
            transport,
            timeout,
            get_auth_message,
            update_state,
        })
    }
}

#[async_trait]
impl<TCredentials, TState, TAuthTransport> AuthenticateLegTrait<TCredentials, TState>
    for AuthenticateLegImpl<TCredentials, TState, TAuthTransport>
where
    TCredentials: Sync,
    TState: Default + Send,
    TAuthTransport: TransportTrait + Send + Sync + 'static,
{
    async fn do_leg(&self, credentials: &TCredentials, state: TState) -> StockTrekResult<TState> {
        let auth_message = (self.get_auth_message)(&self.transport, credentials, &state);
        let reply = self
            .transport
            .send(auth_message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.update_state)(reply, state)
    }
}
