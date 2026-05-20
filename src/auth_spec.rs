use crate::{
    authenticate_leg::AuthenticateLegTrait, authentication_state::AuthenticationState,
    destroy::Destroy, message_leg::MessageLegTrait, session::Session,
};
use stock_trek::error::{
    general::GeneralError,
    result::{StockTrekError, StockTrekResult},
};

pub struct AuthSpec<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
{
    authentication_legs: Vec<Box<dyn AuthenticateLegTrait<TState, TCredentials, TTransports>>>,
    message_leg: Box<dyn MessageLegTrait<TState, TCredentials, TTransports, TMessage, TReply>>,
}

impl<TState, TCredentials, TTransports, TMessage, TReply>
    AuthSpec<TState, TCredentials, TTransports, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        authentication_legs: Vec<Box<dyn AuthenticateLegTrait<TState, TCredentials, TTransports>>>,
        message_leg: Box<dyn MessageLegTrait<TState, TCredentials, TTransports, TMessage, TReply>>,
    ) -> Self {
        Self {
            authentication_legs,
            message_leg,
        }
    }
    pub async fn authenticate(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        session: &mut Session<TState>,
    ) -> StockTrekResult<()> {
        for authentication_leg in &self.authentication_legs {
            match authentication_leg
                .do_leg(credentials, transports, &mut session.state)
                .await
            {
                Err(e) => {
                    session.set_authentication_state(AuthenticationState::AuthenticateFailed);
                    return Err(e);
                }
                _ => {}
            }
        }
        session.set_authentication_state(AuthenticationState::Authenticated);
        Ok(())
    }
    pub async fn sign(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        session: &Session<TState>,
        message: &mut TMessage,
    ) -> StockTrekResult<TReply> {
        {
            let current_authentication_state = session.get_authentication_state();
            if current_authentication_state != AuthenticationState::Authenticated {
                return Err(StockTrekError::General(GeneralError::Message(format!(
                    "Cannot sign message in authentication state {}",
                    current_authentication_state
                ))));
            }
        }
        let reply = match self
            .message_leg
            .do_leg(credentials, transports, &session.state, message)
            .await
        {
            Err(e) => {
                session.set_authentication_state(AuthenticationState::AuthenticateFailed);
                return Err(e);
            }
            Ok(reply) => reply,
        };
        Ok(reply)
    }
}
