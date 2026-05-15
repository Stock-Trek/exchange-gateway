use crate::{auth_spec::AuthSpec, destroy::Destroy, exchange_listener::ExchangeListener};
use stock_trek::error::result::StockTrekResult;

#[allow(dead_code)]
pub struct Authenticator<TState, TCredentials, TTransports, TListener>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TListener: ExchangeListener + 'static,
{
    spec: AuthSpec<TState, TCredentials, TTransports>,
    listener: TListener,
    credentials: TCredentials,
    state: TState,
    transport: TTransports,
}

impl<TState, TCredentials, TTransports, TListener>
    Authenticator<TState, TCredentials, TTransports, TListener>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TListener: ExchangeListener,
{
    pub fn new(
        spec: AuthSpec<TState, TCredentials, TTransports>,
        credentials: TCredentials,
        transports: TTransports,
        listener: TListener,
    ) -> Self {
        Self {
            listener,
            spec,
            credentials,
            state: TState::default(),
            transport: transports,
        }
    }
    pub async fn start(&self) -> StockTrekResult<()> {
        // TODO
        Ok(())
    }
}

impl<TState, TCredentials, TTransports, TListener> Destroy
    for Authenticator<TState, TCredentials, TTransports, TListener>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TListener: ExchangeListener,
{
    fn destroy(&mut self) {
        self.credentials.destroy();
    }
}
