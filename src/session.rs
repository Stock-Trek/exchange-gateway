use crate::{authentication_state::AuthenticationState, destroy::Destroy};
use std::sync::RwLock;

pub struct Session<TState>
where
    TState: Default,
{
    pub state: TState,
    pub authenticate_state: RwLock<AuthenticationState>,
}

impl<TState> Session<TState>
where
    TState: Default,
{
    pub fn new() -> Self {
        Self {
            state: TState::default(),
            authenticate_state: RwLock::new(AuthenticationState::Unauthenticated),
        }
    }
    pub fn get_authentication_state(&self) -> AuthenticationState {
        let read_guard = self.authenticate_state.read().unwrap();
        *read_guard
    }
    pub fn set_authentication_state(&self, authentication_state: AuthenticationState) {
        let mut write_guard = self.authenticate_state.write().unwrap();
        *write_guard = authentication_state;
    }
}

impl<TState> Destroy for Session<TState>
where
    TState: Default,
{
    fn destroy(&mut self) {
        self.set_authentication_state(AuthenticationState::Destroyed);
    }
}
