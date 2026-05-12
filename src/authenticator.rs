use crate::{auth_spec::AuthSpec, destroy::Destroy, exchange_client::ExchangeClient};
use stock_trek::error::result::StockTrekResult;

pub struct Authenticator<TState, TCredentials, TTransports, TClient>
where
    TState: Default,
    TCredentials: Destroy,
    TClient: ExchangeClient,
{
    spec: AuthSpec<TState, TCredentials, TTransports>,
    client: TClient,
    credentials: TCredentials,
    state: TState,
    transport: TTransports,
}

impl<TState, TCredentials, TTransports, TClient>
    Authenticator<TState, TCredentials, TTransports, TClient>
where
    TState: Default,
    TCredentials: Destroy,
    TClient: ExchangeClient,
{
    pub fn new(
        spec: AuthSpec<TState, TCredentials, TTransports>,
        credentials: TCredentials,
        transports: TTransports,
        client: TClient,
    ) -> Self {
        Self {
            client,
            spec,
            credentials,
            state: TState::default(),
            transport: transports,
        }
    }
    pub async fn start(&self) -> StockTrekResult<()> {
        // TODO
    }
}

impl<TState, TCredentials, TTransports, TClient> Destroy
    for Authenticator<TState, TCredentials, TTransports, TClient>
where
    TState: Default,
    TCredentials: Destroy,
    TClient: ExchangeClient,
{
    fn destroy(&mut self) {
        self.credentials.destroy();
    }
}
