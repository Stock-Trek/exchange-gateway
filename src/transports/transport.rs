use crate::{
    error::{EGError, EGResult},
    functions::{FilterMessage, TryConvertRequestTo, TryConvertResponseFrom},
    listeners::{listener::Listener, one_shot_listener::OneShotListener},
    transports::{
        http::{HttpClient, HttpMessageDto},
        websocket::{WebsocketClient, WebsocketMessageDto},
    },
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) struct Transport<TMessageToExchange, TResponse>
where
    TResponse: Send,
{
    transport_client: TransportClient,
    request_to_dto: TryConvertRequestTo<TMessageToExchange, TransportMessageDto>,
    dto_to_response: TryConvertResponseFrom<TransportMessageDto, TResponse>,
    listener: Listener<TResponse>,
    one_shot_listeners: Mutex<Vec<Arc<OneShotListener<TResponse>>>>,
}

pub(crate) enum TransportClient {
    Http(HttpClient),
    Websocket(WebsocketClient),
}

#[derive(Clone)]
pub(crate) enum TransportMessageDto {
    Http(HttpMessageDto),
    Websocket(WebsocketMessageDto),
}

pub fn filter_http_dto(dto: TransportMessageDto) -> EGResult<HttpMessageDto> {
    match dto {
        TransportMessageDto::Http(http_message_dto) => Ok(http_message_dto),
        TransportMessageDto::Websocket(_) => Err(EGError::BadResponse),
    }
}
pub fn filter_websocket_dto(dto: TransportMessageDto) -> EGResult<WebsocketMessageDto> {
    match dto {
        TransportMessageDto::Websocket(websocket_message_dto) => Ok(websocket_message_dto),
        TransportMessageDto::Http(_) => Err(EGError::BadResponse),
    }
}

impl<TMessageToExchange, TResponse> Transport<TMessageToExchange, TResponse>
where
    TResponse: Send,
{
    pub fn new(
        transport_client: TransportClient,
        request_to_dto: TryConvertRequestTo<TMessageToExchange, TransportMessageDto>,
        dto_to_response: TryConvertResponseFrom<TransportMessageDto, TResponse>,
        listener: Listener<TResponse>,
    ) -> Self {
        Self {
            transport_client,
            request_to_dto,
            dto_to_response,
            listener,
            one_shot_listeners: Mutex::new(Vec::new()),
        }
    }
    pub async fn fire_and_forget(
        &self,
        message_to: TMessageToExchange,
        timeout: Duration,
    ) -> EGResult<()> {
        let dto = (self.request_to_dto)(&message_to)?;
        match (&self.transport_client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let http_message_dto = client.send_message(dto, timeout).await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                let response = (self.dto_to_response)(dto)?;
                self.listener.on_message(response).await
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
        filter: &FilterMessage<TResponse, TFiltered>,
    ) -> EGResult<TFiltered>
    where
        TFiltered: Send + Sync,
    {
        let dto = (self.request_to_dto)(&message_to)?;
        match (self.transport_client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let http_message_dto = client.send_message(dto, timeout).await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                let response = (self.dto_to_response)(dto)?;
                filter(&response).ok_or_else(|| EGError::BadResponse)
            }
            (TransportClient::Websocket(client), TransportMessageDto::Websocket(dto)) => {
                client.send_message(dto, timeout).await
            }
            _ => Err(crate::error::EGError::Custom(
                "Transport / DTO type mismatch".into(),
            )),
        }
    }
}
