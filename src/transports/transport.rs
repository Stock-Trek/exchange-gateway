use crate::error::EGResult;
use crate::transports::http_transport::{HttpMessageDto, HttpTransport};
use crate::transports::websocket_transport::{WebsocketMessageDto, WebsocketTransport};
use async_trait::async_trait;
use chrono::Duration;

#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;
    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()>;
}

pub enum Transport {
    Http(HttpTransport),
    Websocket(WebsocketTransport),
}

impl Transport {
    pub async fn send(&self, message_dto: TransportMessageDto, timeout: Duration) -> EGResult<()> {
        match (self, message_dto) {
            (Transport::Http(t), TransportMessageDto::Http(dto)) => t.send(dto, timeout).await,
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                t.send(dto, timeout).await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }
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
                    filter(&TransportMessageDto::Http(resp.clone()))
                })
                .await
            }
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                t.send_and_wait(dto, timeout, move |resp| {
                    filter(&TransportMessageDto::Websocket(resp.clone()))
                })
                .await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }
}

#[derive(Clone)]
pub enum TransportMessageDto {
    Http(HttpMessageDto),
    Websocket(WebsocketMessageDto),
}
