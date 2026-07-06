use crate::{
    error::EGResult,
    functions::FilterMessage,
    transports::{
        http_transport::{HttpMessageDto, HttpTransport},
        websocket_transport::{WebsocketMessageDto, WebsocketTransport},
    },
};
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait TransportTrait: Send + Sync {
    type MessageDto: Send + Sync;

    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()>;

    async fn send_and_wait<TFiltered>(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
        filter: &FilterMessage<Self::MessageDto, TFiltered>,
    ) -> EGResult<TFiltered>
    where
        TFiltered: Send + Sync;
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

    async fn send_and_wait<TFiltered>(
        &self,
        message_dto: Self::MessageDto,
        timeout: Duration,
        filter: &FilterMessage<Self::MessageDto, TFiltered>,
    ) -> EGResult<TFiltered>
    where
        TFiltered: Send + Sync,
    {
        match (self, message_dto) {
            (Transport::Http(t), TransportMessageDto::Http(dto)) => {
                let identity: FilterMessage<HttpMessageDto, HttpMessageDto> =
                    Box::new(|resp| Some(resp.clone()));
                let response = t.send_and_wait_inner(dto, timeout, &identity).await?;
                filter(&TransportMessageDto::Http(response)).ok_or_else(|| {
                    crate::error::EGError::Custom("filter returned None for HTTP response".into())
                })
            }
            (Transport::Websocket(t), TransportMessageDto::Websocket(dto)) => {
                let identity: FilterMessage<WebsocketMessageDto, WebsocketMessageDto> =
                    Box::new(|resp| Some(resp.clone()));
                let response = t.send_and_wait_inner(dto, timeout, &identity).await?;
                filter(&TransportMessageDto::Websocket(response)).ok_or_else(|| {
                    crate::error::EGError::Custom(
                        "filter returned None for Websocket response".into(),
                    )
                })
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
