use crate::{exchange_spec::ExchangeSpecTrait, transports::transport::TransportTrait};
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

#[async_trait]
pub trait MessageLegTrait: Send + Sync {
    type Transports: Send + Sync;
    type Credentials: Send + Sync;
    type State: Default + Send + Sync;
    type TradeRequest: Send + Sync;
    type TradeResponse: Send;

    async fn send_trade_request(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
        state: &Self::State,
        trade_request: &Self::TradeRequest,
    ) -> StockTrekResult<Self::TradeResponse>;
}

pub type MessageLeg<TSpec> = Box<
    dyn MessageLegTrait<
            Transports = <TSpec as ExchangeSpecTrait>::Transports,
            Credentials = <TSpec as ExchangeSpecTrait>::Credentials,
            State = <TSpec as ExchangeSpecTrait>::State,
            TradeRequest = <TSpec as ExchangeSpecTrait>::TradeRequest,
            TradeResponse = <TSpec as ExchangeSpecTrait>::TradeResponse,
        >,
>;

pub type GetTradeMessage<TSpec, TMessage> = fn(
    &<TSpec as ExchangeSpecTrait>::Credentials,
    &<TSpec as ExchangeSpecTrait>::State,
    &<TSpec as ExchangeSpecTrait>::TradeRequest,
) -> StockTrekResult<TMessage>;

pub struct MessageLegImpl<TSpec, TTransport>
where
    TSpec: ExchangeSpecTrait + ?Sized,
    TTransport: TransportTrait,
{
    get_transport: fn(transports: &<TSpec as ExchangeSpecTrait>::Transports) -> &TTransport,
    timeout: Duration,
    get_trade_message: GetTradeMessage<TSpec, <TTransport as TransportTrait>::MessageDto>,
    get_trade_response: fn(
        <TTransport as TransportTrait>::MessageDto,
    ) -> StockTrekResult<<TSpec as ExchangeSpecTrait>::TradeResponse>,
}

impl<TSpec, TTransport> MessageLegImpl<TSpec, TTransport>
where
    TSpec: ExchangeSpecTrait + 'static,
    TTransport: TransportTrait + 'static,
{
    pub fn new(
        get_transport: fn(transports: &<TSpec as ExchangeSpecTrait>::Transports) -> &TTransport,
        timeout: Duration,
        get_trade_message: GetTradeMessage<TSpec, <TTransport as TransportTrait>::MessageDto>,
        get_trade_response: fn(
            <TTransport as TransportTrait>::MessageDto,
        )
            -> StockTrekResult<<TSpec as ExchangeSpecTrait>::TradeResponse>,
    ) -> MessageLeg<TSpec> {
        Box::new(Self {
            get_transport,
            timeout,
            get_trade_message,
            get_trade_response,
        })
    }
}

#[async_trait]
impl<TSpec, TTransport> MessageLegTrait for MessageLegImpl<TSpec, TTransport>
where
    TSpec: ExchangeSpecTrait + 'static,
    TTransport: TransportTrait + 'static,
{
    type Transports = TSpec::Transports;
    type Credentials = TSpec::Credentials;
    type State = TSpec::State;
    type TradeRequest = TSpec::TradeRequest;
    type TradeResponse = TSpec::TradeResponse;

    async fn send_trade_request(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
        state: &Self::State,
        trade_request: &Self::TradeRequest,
    ) -> StockTrekResult<Self::TradeResponse> {
        let transport = (self.get_transport)(transports);
        let message = (self.get_trade_message)(credentials, state, trade_request)?;
        let reply = transport
            .send(message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.get_trade_response)(reply)
    }
}
