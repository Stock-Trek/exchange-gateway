use crate::transport::{http_transport::HttpTransport, websocket_transport::WebsocketTransport};

pub struct HttpWebsocketTransports<THttpMessage, THttpReply, TWebsocketMessage, TWebsocketReply> {
    pub http: Box<dyn HttpTransport<THttpMessage, THttpReply>>,
    pub websocket: Box<dyn WebsocketTransport<TWebsocketMessage, TWebsocketReply>>,
}

impl<THttpMessage, THttpReply, TWebsocketMessage, TWebsocketReply>
    HttpWebsocketTransports<THttpMessage, THttpReply, TWebsocketMessage, TWebsocketReply>
{
    pub fn new(
        http: impl HttpTransport<THttpMessage, THttpReply> + 'static,
        websocket: impl WebsocketTransport<TWebsocketMessage, TWebsocketReply> + 'static,
    ) -> Self {
        Self {
            http: Box::new(http),
            websocket: Box::new(websocket),
        }
    }
}
