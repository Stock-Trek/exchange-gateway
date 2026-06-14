use crate::credentials::Credential;
use crate::transports::transport::TransportTrait;
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub type MessageLeg<TTransports, TState, TTradeRequest, TTradeResponse> =
    Box<dyn MessageLegTrait<TTransports, TState, TTradeRequest, TTradeResponse>>;

#[async_trait]
pub trait MessageLegTrait<TTransports, TState, TTradeRequest, TTradeResponse>: Send + Sync {
    async fn send_trade_request(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
        state: &TState,
        trade_request: &TTradeRequest,
    ) -> StockTrekResult<TTradeResponse>;
}

pub type GetTradeMessage<TState, TTradeRequest, TMessage> =
    fn(&dyn Credential, &TState, &TTradeRequest) -> StockTrekResult<TMessage>;

pub struct MessageLegImpl<TTransports, TState, TTradeRequest, TTradeResponse, TTransport>
where
    TTransport: TransportTrait,
    TState: Default,
{
    get_transport: fn(transports: &TTransports) -> &TTransport,
    timeout: Duration,
    get_trade_message: GetTradeMessage<TState, TTradeRequest, TTransport::MessageDto>,
    get_trade_response: fn(TTransport::MessageDto) -> StockTrekResult<TTradeResponse>,
}

impl<TTransports, TState, TTradeRequest, TTradeResponse, TTransport>
    MessageLegImpl<TTransports, TState, TTradeRequest, TTradeResponse, TTransport>
where
    TTransports: Sync + 'static,
    TState: Default + Sync + 'static,
    TTradeRequest: Sync + 'static,
    TTradeResponse: 'static,
    TTransport: TransportTrait + 'static,
{
    pub fn new(
        get_transport: fn(transports: &TTransports) -> &TTransport,
        timeout: Duration,
        get_trade_message: GetTradeMessage<TState, TTradeRequest, TTransport::MessageDto>,
        get_trade_response: fn(TTransport::MessageDto) -> StockTrekResult<TTradeResponse>,
    ) -> MessageLeg<TTransports, TState, TTradeRequest, TTradeResponse> {
        Box::new(Self {
            get_transport,
            timeout,
            get_trade_message,
            get_trade_response,
        })
    }
}

#[async_trait]
impl<TTransports, TState, TTradeRequest, TTradeResponse, TTransport>
    MessageLegTrait<TTransports, TState, TTradeRequest, TTradeResponse>
    for MessageLegImpl<TTransports, TState, TTradeRequest, TTradeResponse, TTransport>
where
    TTransports: Sync,
    TState: Default + Sync,
    TTradeRequest: Sync,
    TTransport: TransportTrait + 'static,
{
    async fn send_trade_request(
        &self,
        transports: &TTransports,
        credentials: &dyn Credential,
        state: &TState,
        trade_request: &TTradeRequest,
    ) -> StockTrekResult<TTradeResponse> {
        let transport = (self.get_transport)(transports);
        let message = (self.get_trade_message)(credentials, state, trade_request)?;
        let reply = transport
            .send(message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.get_trade_response)(reply)
    }
}
