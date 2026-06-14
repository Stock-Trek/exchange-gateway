use crate::{exchange_spec::ExchangeSpecTrait, transports::transport::TransportTrait};
use async_trait::async_trait;
use chrono::Duration;
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

#[async_trait]
pub trait AuthenticateLegTrait: Send + Sync {
    type Transports: Send + Sync;
    type Credentials: Send + Sync;
    type State: Default + Send + Sync;

    async fn do_leg(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
        state: Self::State,
    ) -> StockTrekResult<Self::State>;
}

pub type AuthenticateLeg<TSpec> = Box<
    dyn AuthenticateLegTrait<
            Transports = <TSpec as ExchangeSpecTrait>::Transports,
            Credentials = <TSpec as ExchangeSpecTrait>::Credentials,
            State = <TSpec as ExchangeSpecTrait>::State,
        >,
>;

pub struct AuthenticateLegImpl<TSpec, TAuthTransport>
where
    TSpec: ExchangeSpecTrait + ?Sized,
    TAuthTransport: TransportTrait,
{
    get_transport: fn(transports: &<TSpec as ExchangeSpecTrait>::Transports) -> &TAuthTransport,
    timeout: Duration,
    get_auth_message: fn(
        &TAuthTransport,
        &<TSpec as ExchangeSpecTrait>::Credentials,
        &<TSpec as ExchangeSpecTrait>::State,
    ) -> TAuthTransport::MessageDto,
    update_state: fn(
        TAuthTransport::MessageDto,
        state: <TSpec as ExchangeSpecTrait>::State,
    ) -> StockTrekResult<<TSpec as ExchangeSpecTrait>::State>,
}

impl<TSpec, TAuthTransport> AuthenticateLegImpl<TSpec, TAuthTransport>
where
    TSpec: ExchangeSpecTrait + 'static,
    TAuthTransport: TransportTrait + 'static,
{
    pub fn new(
        get_transport: fn(transports: &<TSpec as ExchangeSpecTrait>::Transports) -> &TAuthTransport,
        timeout: Duration,
        get_auth_message: fn(
            &TAuthTransport,
            &<TSpec as ExchangeSpecTrait>::Credentials,
            &<TSpec as ExchangeSpecTrait>::State,
        ) -> TAuthTransport::MessageDto,
        update_state: fn(
            TAuthTransport::MessageDto,
            state: <TSpec as ExchangeSpecTrait>::State,
        ) -> StockTrekResult<<TSpec as ExchangeSpecTrait>::State>,
    ) -> AuthenticateLeg<TSpec> {
        Box::new(Self {
            get_transport,
            timeout,
            get_auth_message,
            update_state,
        })
    }
}

#[async_trait]
impl<TSpec, TAuthTransport> AuthenticateLegTrait for AuthenticateLegImpl<TSpec, TAuthTransport>
where
    TSpec: ExchangeSpecTrait + 'static,
    TAuthTransport: TransportTrait + 'static,
{
    type Transports = TSpec::Transports;
    type Credentials = TSpec::Credentials;
    type State = TSpec::State;

    async fn do_leg(
        &self,
        transports: &Self::Transports,
        credentials: &Self::Credentials,
        state: Self::State,
    ) -> StockTrekResult<Self::State> {
        let transport = (self.get_transport)(transports);
        let auth_message = (self.get_auth_message)(transport, credentials, &state);
        let reply = transport
            .send(auth_message, self.timeout)
            .await
            .map_err(|_e| StockTrekError::General(GeneralError::Message("".to_string())))?;
        (self.update_state)(reply, state)
    }
}
