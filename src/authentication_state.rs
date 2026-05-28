use strum::Display;

#[derive(Debug, Display, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationState {
    Unauthenticated,
    Authenticated,
    AuthenticateFailed,
    Destroyed,
}
