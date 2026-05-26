use crate::transport::transport::Transport;

pub type HttpTransport<TMessage, TReply> = Box<dyn HttpTransportTrait<TMessage, TReply>>;

pub trait HttpTransportTrait<TMessage, TReply>: Transport<TMessage, TReply> {}
