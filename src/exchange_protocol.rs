use crate::{
    authenticate_leg::AuthenticateLeg, authentication_state::AuthenticationState, destroy::Destroy,
    message_leg::MessageLeg, session::Session, transport::transport::Transport,
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

pub struct ExchangeProtocol<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    authenticate_legs: Vec<AuthenticateLeg<TState, TCredentials, TTransports>>,
    message_leg: MessageLeg<TState, TCredentials, TTransports, TTransport, TMessage, TReply>,
}

impl<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
    ExchangeProtocol<TState, TCredentials, TTransports, TTransport, TMessage, TReply>
where
    TState: Default + Send + Sync + 'static,
    TCredentials: Destroy + Send + Sync + 'static,
    TTransports: Send + Sync + 'static,
    TTransport: Transport<TMessage, TReply> + Send + Sync + 'static,
    TMessage: Send + Sync + 'static,
    TReply: Send + Sync + 'static,
{
    pub fn new(
        authenticate_legs: Vec<AuthenticateLeg<TState, TCredentials, TTransports>>,
        message_leg: MessageLeg<TState, TCredentials, TTransports, TTransport, TMessage, TReply>,
    ) -> Self {
        Self {
            authenticate_legs,
            message_leg,
        }
    }
    pub async fn authenticate(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
        session: &mut Session<TState>,
    ) -> StockTrekResult<()> {
        for authenticate_leg in &self.authenticate_legs {
            match authenticate_leg
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
    pub async fn send_order_request(
        &self,
        credentials: &TCredentials,
        transports: &TTransports,
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
            .send_order_request(credentials, transports, &session.state, order_request)
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
