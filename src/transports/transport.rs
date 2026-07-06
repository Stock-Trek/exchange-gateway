use crate::error::EGResult;
use crate::transports::http_transport::{HttpMessageDto, HttpTransport};
use crate::transports::websocket_transport::{WebsocketMessageDto, WebsocketTransport};
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;

    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()>;

    async fn send_and_wait<TResponse, F>(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&Self::MessageDto) -> Option<TResponse> + Send + Sync,
        TResponse: Send + Sync;
}

pub enum Transport {
    Http(HttpTransport),
    Websocket(WebsocketTransport),
}

#[async_trait]
impl TransportTrait for Transport {
    type MessageDto = TransportMessageDto;

    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()> {
        match (self, message_dto) {
            (Transport::Http(t), TransportMessageDto::Http(dto)) => {
                TransportTrait::send(t, dto, timeout).await
            }
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                TransportTrait::send(t, dto, timeout).await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }

    async fn send_and_wait<TResponse, F>(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&Self::MessageDto) -> Option<TResponse> + Send + Sync,
        TResponse: Send + Sync,
    {
        match (self, message_dto) {
            (Transport::Http(t), TransportMessageDto::Http(dto)) => {
                TransportTrait::send_and_wait(t, dto, timeout, move |resp| {
                    filter(&TransportMessageDto::Http(resp.clone()))
                })
                .await
            }
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                TransportTrait::send_and_wait(t, dto, timeout, move |resp| {
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
