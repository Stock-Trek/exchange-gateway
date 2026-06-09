pub trait AuthenticationState: private::Sealed {}

pub struct Scratch;
pub struct Unauthenticated;
pub struct Authenticated;
pub struct Destroyed;

mod private {
    pub trait Sealed {}
    impl Sealed for super::Scratch {}
    impl Sealed for super::Unauthenticated {}
    impl Sealed for super::Authenticated {}
    impl Sealed for super::Destroyed {}
}

impl AuthenticationState for Scratch {}
impl AuthenticationState for Unauthenticated {}
impl AuthenticationState for Authenticated {}
impl AuthenticationState for Destroyed {}
