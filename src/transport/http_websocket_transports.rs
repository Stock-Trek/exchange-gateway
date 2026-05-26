use crate::transport::{
    http_transport::{HttpTransport, HttpTransportTrait},
    websocket_transport::{WebsocketTransport, WebsocketTransportTrait},
};

pub struct HttpWebsocketTransports<THttpMessage, THttpReply, TWebsocketMessage, TWebsocketReply> {
    pub http: HttpTransport<THttpMessage, THttpReply>,
    pub websocket: WebsocketTransport<TWebsocketMessage, TWebsocketReply>,
}

impl<THttpMessage, THttpReply, TWebsocketMessage, TWebsocketReply>
    HttpWebsocketTransports<THttpMessage, THttpReply, TWebsocketMessage, TWebsocketReply>
{
    pub fn new(
        http: impl HttpTransportTrait<THttpMessage, THttpReply> + 'static,
        websocket: impl WebsocketTransportTrait<TWebsocketMessage, TWebsocketReply> + 'static,
    ) -> Self {
        Self {
            http: Box::new(http),
            websocket: Box::new(websocket),
        }
    }
}
