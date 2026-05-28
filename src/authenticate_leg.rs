use crate::{destroy::Destroy, transport::transport::TransportTrait};
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub type AuthenticateLeg<TTransports, TCredentials, TState> =
    Box<dyn AuthenticateLegTrait<TTransports, TCredentials, TState>>;

#[async_trait]
pub trait AuthenticateLegTrait<TTransports, TCredentials, TState>: Send + Sync
where
    TCredentials: Destroy,
    TState: Default,
{
    async fn do_leg(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &mut TState,
    ) -> StockTrekResult<()>;
}

pub struct AuthenticateLegImpl<TTransports, TCredentials, TState, TAuthTransport>
where
    TAuthTransport: TransportTrait,
{
    get_transport: fn(transports: &TTransports) -> &TAuthTransport,
    timeout: Duration,
    get_auth_message: fn(&TAuthTransport, &TCredentials, &TState) -> TAuthTransport::MessageDto,
    store_state: fn(TAuthTransport::MessageDto, state: &mut TState) -> StockTrekResult<()>,
}

impl<TTransports, TCredentials, TState, TAuthTransport>
    AuthenticateLegImpl<TTransports, TCredentials, TState, TAuthTransport>
where
    TTransports: Sync + 'static,
    TCredentials: Destroy + Sync + 'static,
    TState: Default + Send + 'static,
    TAuthTransport: TransportTrait + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TAuthTransport,
        timeout: Duration,
        get_auth_message: fn(&TAuthTransport, &TCredentials, &TState) -> TAuthTransport::MessageDto,
        store_state: fn(TAuthTransport::MessageDto, state: &mut TState) -> StockTrekResult<()>,
    ) -> AuthenticateLeg<TTransports, TCredentials, TState> {
        Box::new(Self {
            get_transport,
            timeout,
            get_auth_message,
            store_state,
        })
    }
}

#[async_trait]
impl<TTransports, TCredentials, TState, TAuthTransport>
    AuthenticateLegTrait<TTransports, TCredentials, TState>
    for AuthenticateLegImpl<TTransports, TCredentials, TState, TAuthTransport>
where
    TTransports: Sync,
    TCredentials: Destroy + Sync,
    TState: Default + Send,
    TAuthTransport: TransportTrait + 'static,
{
    async fn do_leg(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        state: &mut TState,
    ) -> StockTrekResult<()> {
        let transport = (self.get_transport)(&transports);
        let auth_message = (self.get_auth_message)(transport, credentials, state);
        let reply = transport
            .send(auth_message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.store_state)(reply, state)
    }
}
