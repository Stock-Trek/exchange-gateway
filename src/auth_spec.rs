use crate::{destroy::Destroy, transport::transport::Transport};
use async_trait::async_trait;
use std::marker::PhantomData;
use stock_trek::error::result::StockTrekResult;

pub struct AuthSpec<TState, TCredentials, TTransports>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
{
    legs: Vec<Box<dyn AuthLegTrait<TState, TCredentials, TTransports>>>,
}

impl<TState, TCredentials, TTransports> AuthSpec<TState, TCredentials, TTransports>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
{
    pub fn new(legs: Vec<Box<dyn AuthLegTrait<TState, TCredentials, TTransports>>>) -> Self {
        Self { legs }
    }
    pub async fn auth(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
    ) -> StockTrekResult<()> {
        let mut state = TState::default();
        for leg in &self.legs {
            leg.do_leg(&mut state, credentials, transports).await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait AuthLegTrait<TState, TCredentials, TTransports> {
    async fn do_leg(
        &self,
        state: &mut TState,
        credentials: &TCredentials,
        transports: &TTransports,
    ) -> StockTrekResult<()>;
}

pub struct AuthLeg<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
where
    TTransport: Transport<TMessagePart, TMessage, TReply> + Send + Sync + 'static,
{
    get_transport: fn(transports: &TTransports) -> &TTransport,
    gather_values: Vec<Box<dyn Fn(&TState, &TCredentials, &mut TMessage) + Send + Sync + 'static>>,
    store_values:
        Vec<Box<dyn Fn(&TReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>>,
    _phantom_message_part: PhantomData<TMessagePart>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
    AuthLeg<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessagePart, TMessage, TReply> + Send + Sync + 'static,
    TMessagePart: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TTransport,
        gather_values: Vec<
            Box<dyn Fn(&TState, &TCredentials, &mut TMessage) + Send + Sync + 'static>,
        >,
        store_values: Vec<
            Box<dyn Fn(&TReply, &mut TState) -> StockTrekResult<()> + Send + Sync + 'static>,
        >,
    ) -> Self {
        Self {
            get_transport,
            gather_values,
            store_values,
            _phantom_message_part: PhantomData,
        }
    }
}

#[async_trait]
impl<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
    AuthLegTrait<TState, TCredentials, TTransports>
    for AuthLeg<TState, TCredentials, TTransports, TTransport, TMessagePart, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessagePart, TMessage, TReply> + Send + Sync + 'static,
    TMessagePart: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    async fn do_leg(
        &self,
        state: &mut TState,
        credentials: &TCredentials,
        transports: &TTransports,
    ) -> StockTrekResult<()> {
        let transport = (self.get_transport)(transports);
        let mut message = TTransport::new_message(transport)?;
        for gather in &self.gather_values {
            gather(state, credentials, &mut message);
        }
        let reply = transport.send_and_wait_for_reply(message).await?;
        for store in &self.store_values {
            store(&reply, state)?;
        }
        Ok(())
    }
}
