use crate::transport::transport::Transport;

pub trait HttpTransport<TMessage, TReply>: Transport<TMessage, TReply> {}

pub struct HttpMessageBuilder<THttpMessage> {
    setter: fn(&THttpMessage),
}

impl<THttpMessage> HttpMessageBuilder<THttpMessage> {
    pub fn new(setter: fn(&THttpMessage)) -> Self {
        Self { setter }
    }
}
