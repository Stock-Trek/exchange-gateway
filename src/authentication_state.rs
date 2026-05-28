pub trait AuthState: private::Sealed {}

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

impl AuthState for Scratch {}
impl AuthState for Unauthenticated {}
impl AuthState for Authenticated {}
impl AuthState for Destroyed {}
