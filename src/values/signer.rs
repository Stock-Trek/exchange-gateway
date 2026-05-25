pub type Signer<TState, TCredentials, TUnsigned, TSigned> =
    Box<dyn SignerTrait<TState, TCredentials, TUnsigned, TSigned>>;

pub trait SignerTrait<TState, TCredentials, TUnsigned, TSigned>: Send + Sync {
    fn sign(&self, state: &TState, credentials: &TCredentials, unsigned: &TUnsigned) -> TSigned;
}
