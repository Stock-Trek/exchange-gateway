use crate::destroy::Destroy;

pub trait Credential: Destroy {
    fn credential(&self) -> Vec<u8>;
}
