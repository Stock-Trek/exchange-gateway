use crate::{destroy::Destroy, transport::transport::Transport};
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

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

pub struct AuthenticateLegGeneric<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
{
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    gather_values: Vec<Box<dyn Fn(&TState, &TCredentials, &mut TMessage) + Send + Sync + 'static>>,
    store_values:
        Vec<Box<dyn Fn(&TReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    AuthenticateLegGeneric<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
        gather_values: Vec<
            Box<dyn Fn(&TState, &TCredentials, &mut TMessage) + Send + Sync + 'static>,
        >,
        store_values: Vec<
            Box<dyn Fn(&TReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>,
        >,
    ) -> Self {
        Self {
            get_transport,
            timeout,
            gather_values,
            store_values,
        }
    }
}

#[async_trait]
impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    AuthenticateLegTrait<TState, TCredentials, TTransports>
    for AuthenticateLegGeneric<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    async fn do_leg(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        state: &mut TState,
    ) -> StockTrekResult<()> {
        let transport = (self.get_transport)(&transports);
        let mut message = TTransport::new_message(transport)?;
        for gather in &self.gather_values {
            gather(&state, &credentials, &mut message);
        }
        let reply = transport
            .send_and_wait_for_reply(&message, self.timeout)
            .await?;
        for store in &self.store_values {
            store(&reply, state)?;
        }
        Ok(())
    }
}
