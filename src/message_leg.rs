use crate::transports::transport::TransportTrait;
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub type MessageLeg<TCredentials, TState, TTradeRequest, TTradeResponse> =
    Box<dyn MessageLegTrait<TCredentials, TState, TTradeRequest, TTradeResponse>>;

#[async_trait]
pub trait MessageLegTrait<TCredentials, TState, TTradeRequest, TTradeResponse>:
    Send + Sync
{
    async fn send_trade_request(
        &self,
        credentials: &TCredentials,
        state: &TState,
        trade_request: &TTradeRequest,
    ) -> StockTrekResult<TTradeResponse>;
}

pub type GetTradeMessage<TCredentials, TState, TTradeRequest, TMessage> =
    fn(&TCredentials, &TState, &TTradeRequest) -> StockTrekResult<TMessage>;

pub struct MessageLegImpl<TCredentials, TState, TTradeRequest, TTradeResponse, TTransport>
where
    TTransport: TransportTrait,
    TState: Default,
{
    transport: TTransport,
    timeout: Duration,
    get_trade_message: GetTradeMessage<TCredentials, TState, TTradeRequest, TTransport::MessageDto>,
    get_trade_response: fn(TTransport::MessageDto) -> StockTrekResult<TTradeResponse>,
}

impl<TCredentials, TState, TTradeRequest, TTradeResponse, TTransport>
    MessageLegImpl<TCredentials, TState, TTradeRequest, TTradeResponse, TTransport>
where
    TCredentials: Sync + 'static,
    TState: Default + Sync + 'static,
    TTradeRequest: Sync + 'static,
    TTradeResponse: 'static,
    TTransport: TransportTrait + Send + Sync + 'static,
{
    pub fn new(
        transport: TTransport,
        timeout: Duration,
        get_trade_message: GetTradeMessage<
            TCredentials,
            TState,
            TTradeRequest,
            TTransport::MessageDto,
        >,
        get_trade_response: fn(TTransport::MessageDto) -> StockTrekResult<TTradeResponse>,
    ) -> MessageLeg<TCredentials, TState, TTradeRequest, TTradeResponse> {
        Box::new(Self {
            transport,
            timeout,
            get_trade_message,
            get_trade_response,
        })
    }
}

#[async_trait]
impl<TCredentials, TState, TTradeRequest, TTradeResponse, TTransport>
    MessageLegTrait<TCredentials, TState, TTradeRequest, TTradeResponse>
    for MessageLegImpl<TCredentials, TState, TTradeRequest, TTradeResponse, TTransport>
where
    TCredentials: Sync,
    TState: Default + Sync,
    TTradeRequest: Sync,
    TTransport: TransportTrait + Send + Sync + 'static,
{
    async fn send_trade_request(
        &self,
        credentials: &TCredentials,
        state: &TState,
        trade_request: &TTradeRequest,
    ) -> StockTrekResult<TTradeResponse> {
        let message = (self.get_trade_message)(credentials, state, trade_request)?;
        let reply = self
            .transport
            .send(message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.get_trade_response)(reply)
    }
}
