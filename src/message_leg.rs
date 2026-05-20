use std::marker::PhantomData;

use crate::{destroy::Destroy, transport::transport::Transport};
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::result::StockTrekResult;

#[async_trait]
pub trait MessageLegTrait<TState, TCredentials, TTransports, TMessage, TReply>:
    Send + Sync
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    async fn do_leg(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        state: &TState,
        message: &mut TMessage,
    ) -> StockTrekResult<TReply>;
}

pub struct MessageLegGeneric<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
{
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    gather_signatures: Vec<
        Box<
            dyn Fn(&TState, &TCredentials, &mut TMessage) -> StockTrekResult<()>
                + Send
                + Sync
                + 'static,
        >,
    >,
    _phantom_reply: PhantomData<TReply>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    MessageLegGeneric<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
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
        gather_signatures: Vec<
            Box<
                dyn Fn(&TState, &TCredentials, &mut TMessage) -> StockTrekResult<()>
                    + Send
                    + Sync
                    + 'static,
            >,
        >,
    ) -> Self {
        Self {
            get_transport,
            timeout,
            gather_signatures,
            _phantom_reply: PhantomData,
        }
    }
}

#[async_trait]
impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    MessageLegTrait<TState, TCredentials, TTransports, TMessage, TReply>
    for MessageLegGeneric<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
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
        state: &TState,
        message: &mut TMessage,
    ) -> StockTrekResult<TReply> {
        let transport = (self.get_transport)(&transports);
        for gather in &self.gather_signatures {
            gather(&state, &credentials, message);
        }
        let reply = transport
            .send_and_wait_for_reply(message, self.timeout)
            .await?;
        Ok(reply)
    }
}
