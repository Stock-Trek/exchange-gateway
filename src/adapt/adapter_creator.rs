use crate::{adapt::adapter::Adapter, destroy::Destroy};

pub type AdapterCreator<TCredentials, TTransports> =
    Box<dyn AdapterCreatorTrait<TCredentials, TTransports>>;

pub trait AdapterCreatorTrait<TCredentials, TTransports>
where
    TCredentials: Destroy,
{
    fn create_adapter(&self, credentials: TCredentials, transports: TTransports) -> Adapter;
}
