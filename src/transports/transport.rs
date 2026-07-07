use crate::{
    error::{EGError, EGResult},
    functions::{FilterMessage, TryConvertRequestTo, TryConvertResponseFrom},
    listeners::listener::Listener,
    transports::{
        http_client::{HttpClient, HttpMessageDto},
        websocket_client::{WebsocketClient, WebsocketMessageDto},
    },
};
use std::time::Duration;

pub(crate) struct Transport<TMessageToExchange, TMessageFromExchange> {
    pub client: TransportClient,
    pub request_to_dto: TryConvertRequestTo<TMessageToExchange, TransportMessageDto>,
    pub dto_to_response: TryConvertResponseFrom<TransportMessageDto, TMessageFromExchange>,
    pub listener: Listener<TransportMessageDto>,
}

pub(crate) enum TransportClient {
    Http(HttpClient),
    Websocket(WebsocketClient),
}

#[derive(Clone)]
pub enum TransportMessageDto {
    Http(HttpMessageDto),
    Websocket(WebsocketMessageDto),
}

impl<TMessageToExchange, TMessageFromExchange> Transport<TMessageToExchange, TMessageFromExchange> {
    pub async fn send(&self, message_to: TMessageToExchange, timeout: Duration) -> EGResult<()> {
        let dto = (self.request_to_dto)(&message_to)?;
        match (self.client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let future = client.send_message(dto, timeout);
                let http_message_dto = future.await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                self.listener.on_message(dto).await
            }
            (TransportClient::Websocket(client), TransportMessageDto::Websocket(dto)) => {
                client.send_message(dto, timeout).await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }
    pub async fn send_and_wait<TFiltered>(
        &self,
        message_to: TMessageToExchange,
        timeout: Duration,
        filter: &FilterMessage<TMessageFromExchange, TFiltered>,
    ) -> EGResult<TFiltered>
    where
        TFiltered: Send + Sync,
    {
        let dto = (self.request_to_dto)(&message_to)?;
        match (self.client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let http_message_dto = client.send_message(dto, timeout).await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                let message_from = (self.dto_to_response)(dto)?;
                filter(&message_from).ok_or_else(|| EGError::BadResponse)
            }
            (TransportClient::Websocket(client), TransportMessageDto::Websocket(dto)) => {
                client.send_message(dto, timeout).await
                // let identity: FilterMessage<WebsocketMessageDto, WebsocketMessageDto> =
                //     Box::new(|resp| Some(resp.clone()));
                // let response = t.send_and_wait_inner(dto, timeout, &identity).await?;
                // filter(&TransportMessageDto::Websocket(response)).ok_or_else(|| {
                //     crate::error::EGError::Custom(
                //         "filter returned None for Websocket response".into(),
                //     )
                // })
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }
}
