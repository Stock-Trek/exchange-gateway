use crate::credentials::Credential;
use crate::transports::transport::TransportTrait;
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub type AuthenticateLeg<TTransports, TState> = Box<dyn AuthenticateLegTrait<TTransports, TState>>;

#[async_trait]
pub trait AuthenticateLegTrait<TTransports, TState>: Send + Sync
where
    TState: Default,
{
    async fn do_leg(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
        state: TState,
    ) -> StockTrekResult<TState>;
}

pub struct AuthenticateLegImpl<TTransports, TState, TAuthTransport>
where
    TAuthTransport: TransportTrait,
{
    get_transport: fn(transports: &TTransports) -> &TAuthTransport,
    timeout: Duration,
    get_auth_message: fn(&TAuthTransport, &dyn Credential, &TState) -> TAuthTransport::MessageDto,
    update_state: fn(TAuthTransport::MessageDto, state: TState) -> StockTrekResult<TState>,
}

impl<TTransports, TState, TAuthTransport> AuthenticateLegImpl<TTransports, TState, TAuthTransport>
where
    TTransports: Sync + 'static,
    TState: Default + Send + 'static,
    TAuthTransport: TransportTrait + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TAuthTransport,
        timeout: Duration,
        get_auth_message: fn(
            &TAuthTransport,
            &dyn Credential,
            &TState,
        ) -> TAuthTransport::MessageDto,
        update_state: fn(TAuthTransport::MessageDto, state: TState) -> StockTrekResult<TState>,
    ) -> AuthenticateLeg<TTransports, TState> {
        Box::new(Self {
            get_transport,
            timeout,
            get_auth_message,
            update_state,
        })
    }
}

#[async_trait]
impl<TTransports, TState, TAuthTransport> AuthenticateLegTrait<TTransports, TState>
    for AuthenticateLegImpl<TTransports, TState, TAuthTransport>
where
    TTransports: Sync,
    TState: Default + Send,
    TAuthTransport: TransportTrait + 'static,
{
    async fn do_leg(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
        state: TState,
    ) -> StockTrekResult<TState> {
        let transport = (self.get_transport)(transports);
        let auth_message = (self.get_auth_message)(transport, credentials, &state);
        let reply = transport
            .send(auth_message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.update_state)(reply, state)
    }
}
