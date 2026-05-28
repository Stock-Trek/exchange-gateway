/// Sealed trait for authentication states.
/// Only the four state structs below implement this trait.
pub trait AuthState: private::Sealed {}

pub struct Unauthenticated;
pub struct Authenticated;
pub struct AuthenticateFailed;
pub struct Destroyed;

mod private {
    pub trait Sealed {}
    impl Sealed for super::Unauthenticated {}
    impl Sealed for super::Authenticated {}
    impl Sealed for super::AuthenticateFailed {}
    impl Sealed for super::Destroyed {}
}

impl AuthState for Unauthenticated {}
impl AuthState for Authenticated {}
impl AuthState for AuthenticateFailed {}
impl AuthState for Destroyed {}
