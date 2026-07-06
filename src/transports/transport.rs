use crate::error::EGResult;
use crate::listeners::listener::Listener;
use crate::transports::http_transport::{HttpMessageDto, HttpTransport as HttpTransportInner};
use crate::transports::websocket_transport::{
    WebsocketMessageDto, WebsocketTransport as WebsocketTransportInner,
};
use async_trait::async_trait;
use chrono::Duration;
use std::sync::Arc;

/// The existing trait, kept for backward compatibility.
#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;
    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()>;
}

// ---------------------------------------------------------------------------
// Transport enum
// ---------------------------------------------------------------------------

/// A concrete enum over the available transport kinds.
///
/// Each variant carries the corresponding DTO type:
/// - `Transport::Http` accepts [`HttpMessageDto`]
/// - `Transport::Websocket` accepts [`WebsocketMessageDto`]
pub enum Transport {
    Http(HttpTransport),
    Websocket(WebsocketTransport),
}

impl Transport {
    /// Fire-and-forget send. Returns `EGResult<()>`.
    pub async fn send(&self, message_dto: TransportMessageDto, timeout: Duration) -> EGResult<()> {
        match (self, message_dto) {
            (Transport::Http(t), TransportMessageDto::Http(dto)) => {
                TransportTrait::send(&**t, dto, timeout).await
            }
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                TransportTrait::send(&**t, dto, timeout).await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }

    /// Send and wait for a response that matches the given filter.
    ///
    /// The `filter` closure is called with the response DTO and should return
    /// `Some(value)` when a matching response is received. Returns the value
    /// produced by the filter, or an error on timeout / mismatch.
    pub async fn send_and_wait<TResponse, F>(
        &self,
        message_dto: TransportMessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&TransportMessageDto) -> Option<TResponse> + Send + Sync,
        TResponse: Send + Sync,
    {
        match (self, message_dto) {
            (Transport::Http(t), TransportMessageDto::Http(dto)) => {
                t.send_and_wait(dto, timeout, move |resp| {
                    filter(&TransportMessageDto::Http(resp))
                })
                .await
            }
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                t.send_and_wait(dto, timeout, move |resp| {
                    filter(&TransportMessageDto::Websocket(resp))
                })
                .await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }
}

/// Unified DTO enum for the [`Transport`] enum.
#[derive(Clone)]
pub enum TransportMessageDto {
    Http(HttpMessageDto),
    Websocket(WebsocketMessageDto),
}

// ---------------------------------------------------------------------------
// Cloneable wrapper around the (pub(crate)) inner transports.
// ---------------------------------------------------------------------------

/// Cloneable wrapper around [`HttpTransportInner`] that enables use in an enum.
#[derive(Clone)]
pub struct HttpTransport(pub(crate) Arc<HttpTransportInner>);

impl HttpTransport {
    /// Wraps the inner transport.
    pub fn new(
        client: crate::transports::http_transport::HttpClient,
        listener: Listener<HttpMessageDto>,
    ) -> Self {
        Self(Arc::new(HttpTransportInner { client, listener }))
    }

    /// Return a reference to the inner transport.
    pub fn inner(&self) -> &HttpTransportInner {
        &self.0
    }

    /// Send and wait — for HTTP the response comes back synchronously from
    /// `send_message`, so we can filter it directly.
    pub async fn send_and_wait<TResponse, F>(
        &self,
        dto: HttpMessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&HttpMessageDto) -> Option<TResponse> + Send + Sync,
    {
        let response = self.0.client.send_message(dto, timeout).await?;
        filter(&response).ok_or_else(|| {
            crate::error::EGError::Custom("filter returned None for HTTP response".into())
        })
    }
}

impl std::ops::Deref for HttpTransport {
    type Target = HttpTransportInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Cloneable wrapper around [`WebsocketTransportInner`].
#[derive(Clone)]
pub struct WebsocketTransport(pub(crate) Arc<WebsocketTransportInner>);

impl WebsocketTransport {
    /// Wraps the inner transport.
    pub fn new(client: crate::transports::websocket_transport::WebsocketClient) -> Self {
        Self(Arc::new(WebsocketTransportInner { client }))
    }

    /// Return a reference to the inner transport.
    pub fn inner(&self) -> &WebsocketTransportInner {
        &self.0
    }

    /// Send and wait for a matching websocket response.
    ///
    /// **Note:** A real implementation would require the `WebsocketClient` to
    /// push incoming messages into a shared channel that can be polled here.
    /// See [`QueueListener`] for a candidate.
    pub async fn send_and_wait<TResponse, F>(
        &self,
        dto: WebsocketMessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&WebsocketMessageDto) -> Option<TResponse> + Send + Sync,
    {
        self.0.client.send_message(dto, timeout).await?;

        Err(crate::error::EGError::Custom(
            "send_and_wait for Websocket requires a response queue; \
             pass a QueueListener<WebsocketMessageDto> during construction \
             and poll it here"
                .into(),
        ))
    }
}

impl std::ops::Deref for WebsocketTransport {
    type Target = WebsocketTransportInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
