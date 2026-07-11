use crate::{
    error::{EGError, EGResult},
    functions::{ArcTryConvertValue, double_converter},
    listeners::{
        convert_listener::ConvertListener, listener::Listener,
        websocket_listener::WebsocketListener,
    },
    transports::{
        http::{HttpClientMarker, HttpMessageDto},
        websocket::{WebsocketClientMarker, WebsocketMessageDto},
    },
};
use std::{sync::Arc, time::Duration};

pub(crate) struct Transport<TMessageToExchange, TMessageFromExchange, TResponse>
where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    transport_client: TransportClient,
    request_to_dto: ArcTryConvertValue<TMessageToExchange, TransportMessageDto>,
    dto_to_message_from: ArcTryConvertValue<TransportMessageDto, TMessageFromExchange>,
    message_from_to_response: ArcTryConvertValue<TMessageFromExchange, TResponse>,
    listener: Listener<TResponse>,
    websocket_listener: WebsocketListener<TMessageFromExchange>,
}

pub(crate) enum TransportClient {
    Http(HttpClientMarker),
    Websocket(WebsocketClientMarker),
}

#[derive(Clone)]
pub(crate) enum TransportMessageDto {
    Http(HttpMessageDto),
    Websocket(WebsocketMessageDto),
}

pub(crate) fn filter_http_dto(dto: TransportMessageDto) -> EGResult<HttpMessageDto> {
    match dto {
        TransportMessageDto::Http(http_message_dto) => Ok(http_message_dto),
        TransportMessageDto::Websocket(_) => Err(EGError::BadResponse),
    }
}
pub(crate) fn filter_websocket_dto(dto: TransportMessageDto) -> EGResult<WebsocketMessageDto> {
    match dto {
        TransportMessageDto::Websocket(websocket_message_dto) => Ok(websocket_message_dto),
        TransportMessageDto::Http(_) => Err(EGError::BadResponse),
    }
}

impl<TMessageToExchange, TMessageFromExchange, TResponse>
    Transport<TMessageToExchange, TMessageFromExchange, TResponse>
where
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub fn new(
        transport_client: TransportClient,
        request_to_dto: ArcTryConvertValue<TMessageToExchange, TransportMessageDto>,
        dto_to_message_from: ArcTryConvertValue<TransportMessageDto, TMessageFromExchange>,
        message_from_to_response: ArcTryConvertValue<TMessageFromExchange, TResponse>,
        listener: Listener<TResponse>,
    ) -> Self {
        let websocket_delegate: Listener<TMessageFromExchange> = Arc::new(ConvertListener::new(
            message_from_to_response.clone(),
            listener.clone(),
        ));
        let websocket_listener =
            WebsocketListener::new(dto_to_message_from.clone(), websocket_delegate);
        Self {
            transport_client,
            request_to_dto,
            dto_to_message_from,
            message_from_to_response,
            listener,
            websocket_listener,
        }
    }
    pub async fn fire_and_forget(
        &self,
        message_to: TMessageToExchange,
        timeout: Duration,
    ) -> EGResult<()> {
        let dto = (self.request_to_dto)(message_to)?;
        match (&self.transport_client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let http_message_dto = client.send_message(dto, timeout).await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                let message_from = (self.dto_to_message_from)(dto)?;
                let response = (self.message_from_to_response)(message_from)?;
                self.listener.on_message(response).await
            }
            (TransportClient::Websocket(client), TransportMessageDto::Websocket(dto)) => {
                client.send_message(dto, timeout).await
            }
            _ => Err(crate::error::EGError::BadResponse),
        }
    }
    pub async fn send_and_wait_for_message_from<TFiltered>(
        &self,
        message_to: TMessageToExchange,
        timeout: Duration,
        converter: ArcTryConvertValue<TMessageFromExchange, TFiltered>,
    ) -> EGResult<TFiltered>
    where
        TFiltered: Send + Sync + 'static,
    {
        let dto = (self.request_to_dto)(message_to)?;
        match (&self.transport_client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let http_message_dto = client.send_message(dto, timeout).await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                let message_from = (self.dto_to_message_from)(dto)?;
                converter(message_from)
            }
            (TransportClient::Websocket(client), TransportMessageDto::Websocket(dto)) => {
                client.send_message(dto, timeout).await?;
                self.websocket_listener
                    .wait_for_converted_response(converter)
                    .await
            }
            _ => Err(EGError::BadResponse),
        }
    }
    pub async fn send_and_wait_for_response<TFiltered>(
        &self,
        message_to: TMessageToExchange,
        timeout: Duration,
        converter: ArcTryConvertValue<TResponse, TFiltered>,
    ) -> EGResult<TFiltered>
    where
        TFiltered: Send + Sync + 'static,
    {
        let dto = (self.request_to_dto)(message_to)?;
        match (&self.transport_client, dto) {
            (TransportClient::Http(client), TransportMessageDto::Http(dto)) => {
                let http_message_dto = client.send_message(dto, timeout).await?;
                let dto = TransportMessageDto::Http(http_message_dto);
                let message_from = (self.dto_to_message_from)(dto)?;
                let response = (self.message_from_to_response)(message_from)?;
                converter(response)
            }
            (TransportClient::Websocket(client), TransportMessageDto::Websocket(dto)) => {
                let response_converter =
                    double_converter(self.message_from_to_response.clone(), converter);
                client.send_message(dto, timeout).await?;
                self.websocket_listener
                    .wait_for_converted_response(response_converter)
                    .await
            }
            _ => Err(EGError::BadResponse),
        }
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        match &self.transport_client {
            TransportClient::Websocket(client) => client.disconnect().await,
            _ => Ok(()),
        }
    }
}
