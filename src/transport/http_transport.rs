use crate::transport::transport::Transport;
use std::marker::PhantomData;

pub trait HttpTransport<TMessage, TReply>: Transport<HttpPart, TMessage, TReply> {}

pub enum HttpPart {
    Header(String),
    QueryParam(String),
    BodyPart { json_path: String },
}

pub struct HttpMessageBuilder<THttpMessage, TSetter>
where
    TSetter: Fn(&THttpMessage, HttpPart),
{
    _phantom: PhantomData<THttpMessage>,
    setter: TSetter,
}

impl<THttpMessage, TSetter> HttpMessageBuilder<THttpMessage, TSetter>
where
    TSetter: Fn(&THttpMessage, HttpPart),
{
    pub fn new(setter: TSetter) -> Self {
        Self {
            _phantom: PhantomData,
            setter,
        }
    }
}
