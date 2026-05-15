use crate::transport::transport::Transport;

pub trait HttpTransport<TMessage, TReply>: Transport<Message = TMessage, Reply = TReply> {}

pub struct HttpMessageBuilder<THttpMessage> {
    #[allow(dead_code)]
    setter: fn(&THttpMessage),
}

impl<THttpMessage> HttpMessageBuilder<THttpMessage> {
    pub fn new(setter: fn(&THttpMessage)) -> Self {
        Self { setter }
    }
}
