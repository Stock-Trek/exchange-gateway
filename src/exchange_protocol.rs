use crate::{
    authenticate_leg::AuthenticateLeg, authentication_state::AuthenticationState, destroy::Destroy,
    message_leg::MessageLeg, session::Session,
};
use rust_decimal::Decimal;
use stock_trek::{
    asset_id::AssetId,
    error::{
        general::GeneralError,
        result::{StockTrekError, StockTrekResult},
    },
    order::{order_request::OrderRequest, order_response::OrderResponse},
};

pub struct ExchangeProtocol<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
    message_leg: MessageLeg<TTransports, TCredentials, TState>,
}

impl<TTransports, TCredentials, TState> ExchangeProtocol<TTransports, TCredentials, TState>
where
    TCredentials: Destroy,
    TState: Default,
{
    pub fn new(
        authenticate_legs: Vec<AuthenticateLeg<TTransports, TCredentials, TState>>,
        message_leg: MessageLeg<TTransports, TCredentials, TState>,
    ) -> Self {
        Self {
            authenticate_legs,
            message_leg,
        }
    }
    pub async fn authenticate(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        session: &mut Session<TState>,
    ) -> StockTrekResult<()> {
        for authenticate_leg in &self.authenticate_legs {
            match authenticate_leg
                .do_leg(transports, credentials, &mut session.state)
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
    pub async fn send_order_request(
        &self,
        transports: &TTransports,
        credentials: &TCredentials,
        session: &Session<TState>,
        order_request: OrderRequest<AssetId, Decimal>,
    ) -> StockTrekResult<OrderResponse> {
        {
            let current_authentication_state = session.get_authentication_state();
            if current_authentication_state != AuthenticationState::Authenticated {
                return Err(StockTrekError::General(GeneralError::Message(format!(
                    "Cannot sign message in authentication state {}",
                    current_authentication_state
                ))));
            }
        }
        let response = match self
            .message_leg
            .send_order_request(transports, credentials, &session.state, order_request)
            .await
        {
            Err(e) => {
                session.set_authentication_state(AuthenticationState::AuthenticateFailed);
                return Err(e);
            }
            Ok(response) => response,
        };
        Ok(response)
    }
}
