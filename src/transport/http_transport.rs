use crate::transport::transport::Transport;

pub trait HttpTransport<TMessage, TReply>: Transport<TMessage, TReply> {}
