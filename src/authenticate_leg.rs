use crate::{
    destroy::Destroy,
    transport::transport::Transport,
    values::{auth_message::AuthMessageExtractor, store_auth_value::StoreAuthValue},
};
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

pub type AuthenticateLeg<TState, TCredentials, TTransports> =
    Box<dyn AuthenticateLegTrait<TState, TCredentials, TTransports>>;

#[async_trait]
pub trait AuthenticateLegTrait<TState, TCredentials, TTransports>: Send + Sync
where
    TState: Default + Send + Sync,
    TCredentials: Destroy + Send + Sync,
{
    async fn do_leg(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        state: &mut TState,
    ) -> StockTrekResult<()>;
}

pub struct AuthenticateLegImpl<
    TState,
    TCredentials,
    TTransports,
    TAuthTransport,
    TAuthMessage,
    TAuthReply,
> where
    TAuthTransport: Transport<TAuthMessage, TAuthReply> + 'static,
{
    get_transport: fn(transports: &TTransports) -> &TAuthTransport,
    timeout: Duration,
    auth_message_extractor:
        AuthMessageExtractor<TState, TCredentials, TAuthTransport, TAuthMessage>,
    store_values: Vec<StoreAuthValue<TAuthReply, TState>>,
}

impl<TState, TCredentials, TTransports, TAuthTransport, TAuthMessage, TAuthReply>
    AuthenticateLegImpl<TState, TCredentials, TTransports, TAuthTransport, TAuthMessage, TAuthReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TAuthTransport: Transport<TAuthMessage, TAuthReply> + 'static,
    TAuthMessage: Send + Sync + 'static,
    TAuthReply: Send + Sync + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TAuthTransport,
        timeout: Duration,
        auth_message_extractor: AuthMessageExtractor<
            TState,
            TCredentials,
            TAuthTransport,
            TAuthMessage,
        >,
        store_values: Vec<StoreAuthValue<TAuthReply, TState>>,
    ) -> AuthenticateLeg<TState, TCredentials, TTransports> {
        Box::new(Self {
            get_transport,
            timeout,
            auth_message_extractor,
            store_values,
        })
    }
}

#[async_trait]
impl<TState, TCredentials, TTransports, TAuthTransport, TAuthMessage, TAuthReply>
    AuthenticateLegTrait<TState, TCredentials, TTransports>
    for AuthenticateLegImpl<
        TState,
        TCredentials,
        TTransports,
        TAuthTransport,
        TAuthMessage,
        TAuthReply,
    >
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TAuthTransport: Transport<TAuthMessage, TAuthReply> + 'static,
    TAuthMessage: Send + Sync + 'static,
    TAuthReply: Send + Sync + 'static,
{
    async fn do_leg(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        state: &mut TState,
    ) -> StockTrekResult<()> {
        let transport = (self.get_transport)(&transports);
        let auth_message = self
            .auth_message_extractor
            .extract(state, credentials, transport);
        let reply = transport
            .send_and_wait_for_reply(&auth_message, self.timeout)
            .await?;
        for store in &self.store_values {
            store.store_auth_value(&reply, state)?;
        }
        Ok(())
    }
}
