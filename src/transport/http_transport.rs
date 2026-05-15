use crate::transport::transport::Transport;

pub trait HttpTransport<TMessage, TReply>: Transport<HttpPart, TMessage, TReply> {}

pub enum HttpPart {
    Header(String),
    QueryParam(String),
    BodyPart { json_path: String },
}

pub struct HttpMessageBuilder<THttpMessage> {
    setter: fn(&THttpMessage, HttpPart),
}

impl<THttpMessage> HttpMessageBuilder<THttpMessage> {
    pub fn new(setter: fn(&THttpMessage, HttpPart)) -> Self {
        Self { setter }
    }
}
