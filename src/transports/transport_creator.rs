use crate::{
    error::EGResult, listeners::listener::Listener, transports::transport::TransportTrait,
};
use std::sync::Arc;

pub type TransportCreator<TTransport, TMessageDto> =
    Box<dyn TransportCreatorTrait<TTransport, TMessageDto>>;

pub trait TransportCreatorTrait<TTransport, TMessageDto>
where
    TTransport: TransportTrait<MessageDto = TMessageDto>,
{
    fn create_transport(&self, listener: Arc<Listener<TMessageDto>>) -> EGResult<TTransport>;
}
