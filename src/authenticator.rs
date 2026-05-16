use crate::{auth_spec::AuthSpec, destroy::Destroy, exchange_listener::ExchangeListener};
use stock_trek::error::result::StockTrekResult;

#[allow(dead_code)]
pub struct Authenticator<TListener, TState, TCredentials, TTransports>
where
    TListener: ExchangeListener + 'static,
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
{
    spec: AuthSpec<TState, TCredentials, TTransports>,
    listener: TListener,
    state: TState,
    credentials: TCredentials,
    transports: TTransports,
}

impl<TListener, TState, TCredentials, TTransports>
    Authenticator<TListener, TState, TCredentials, TTransports>
where
    TListener: ExchangeListener,
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
{
    pub fn new(
        spec: AuthSpec<TState, TCredentials, TTransports>,
        listener: TListener,
        credentials: TCredentials,
        transports: TTransports,
    ) -> Self {
        Self {
            listener,
            spec,
            state: TState::default(),
            credentials,
            transports,
        }
    }
    pub async fn start(&self) -> StockTrekResult<()> {
        let mut state = TState::default();
        self.spec
            .auth(&mut state, &self.credentials, &self.transports)
            .await?;
        // TODO
        Ok(())
    }
}

impl<TListener, TState, TCredentials, TTransports> Destroy
    for Authenticator<TListener, TState, TCredentials, TTransports>
where
    TListener: ExchangeListener,
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
{
    fn destroy(&mut self) {
        self.credentials.destroy();
    }
}
