use crate::{auth_leg::AuthLeg, destroy::Destroy};
use stock_trek::error::result::StockTrekResult;

pub struct AuthSpec<TState, TCredentials, TTransports>
where
    TState: Default,
    TCredentials: Destroy,
{
    legs: Vec<Box<dyn AuthLeg<TState, TCredentials, TTransports>>>,
}

impl<TCredentials, TState, TTransports> AuthSpec<TState, TCredentials, TTransports>
where
    TState: Default,
    TCredentials: Destroy,
{
    async fn auth(
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
