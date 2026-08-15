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
use std::{fmt::Display, sync::Arc, time::Duration};
use strum::Display;

pub(crate) struct Transport<TMessageToExchange, TTransportBody, TMessageFromExchange, TResponse>
where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    transport_client: TransportClient<TTransportBody>,
    request_to_dto: ArcTryConvertValue<TMessageToExchange, TransportMessageDto<TTransportBody>>,
    dto_to_message_from:
        ArcTryConvertValue<TransportMessageDto<TTransportBody>, TMessageFromExchange>,
    message_from_to_response: ArcTryConvertValue<TMessageFromExchange, TResponse>,
    listener: Listener<TResponse>,
    websocket_listener: WebsocketListener<TTransportBody, TMessageFromExchange>,
}

#[derive(Debug, Display, Clone, Copy)]
pub enum TransportType {
    Http,
    Websocket,
}

pub(crate) enum TransportClient<T> {
    Http(HttpClientMarker<T>),
    Websocket(WebsocketClientMarker<T>),
}

#[derive(Debug, Clone)]
pub enum TransportMessageDto<T> {
    Http(HttpMessageDto<T>),
    Websocket(WebsocketMessageDto<T>),
}

impl<TMessageToExchange, TTransportBody, TMessageFromExchange, TResponse> std::fmt::Debug
    for Transport<TMessageToExchange, TTransportBody, TMessageFromExchange, TResponse>
where
    TMessageFromExchange: Send,
    TResponse: Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport")
            .field("transport_client", &self.transport_client)
            .field("request_to_dto", &"<function>")
            .field("dto_to_message_from", &"<function>")
            .field("message_from_to_response", &"<function>")
            .field("listener", &"<Listener>")
            .field("websocket_listener", &self.websocket_listener)
            .finish()
    }
}

impl<T> TransportClient<T> {
    pub fn transport_type(&self) -> TransportType {
        match self {
            Self::Http(..) => TransportType::Http,
            Self::Websocket(..) => TransportType::Websocket,
        }
    }
}

impl<TTransportBody> std::fmt::Debug for TransportClient<TTransportBody> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            Self::Http(..) => "http",
            Self::Websocket(..) => "websocket",
        };
        f.debug_struct("TransportClient")
            .field("variant", &variant)
            .finish()
    }
}

impl<TTransportBody> std::fmt::Display for TransportMessageDto<TTransportBody>
where
    TTransportBody: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(http) => http.fmt(f),
            Self::Websocket(websocket) => websocket.fmt(f),
        }
    }
}

pub(crate) fn filter_http_dto<TTransportBody>(
    dto: TransportMessageDto<TTransportBody>,
) -> EGResult<HttpMessageDto<TTransportBody>> {
    match dto {
        TransportMessageDto::Http(http_message_dto) => Ok(http_message_dto),
        TransportMessageDto::Websocket(_) => {
            Err(EGError::BadTransportType(TransportType::Websocket))
        }
    }
}
pub(crate) fn filter_websocket_dto<TTransportBody>(
    dto: TransportMessageDto<TTransportBody>,
) -> EGResult<WebsocketMessageDto<TTransportBody>> {
    match dto {
        TransportMessageDto::Websocket(websocket_message_dto) => Ok(websocket_message_dto),
        TransportMessageDto::Http(_) => Err(EGError::BadTransportType(TransportType::Http)),
    }
}

impl<TMessageToExchange, TTransportBody, TMessageFromExchange, TResponse>
    Transport<TMessageToExchange, TTransportBody, TMessageFromExchange, TResponse>
where
    TMessageFromExchange: Send + Sync + 'static,
    TResponse: Send + Sync + 'static,
{
    pub fn new(
        transport_client: TransportClient<TTransportBody>,
        request_to_dto: ArcTryConvertValue<TMessageToExchange, TransportMessageDto<TTransportBody>>,
        dto_to_message_from: ArcTryConvertValue<
            TransportMessageDto<TTransportBody>,
            TMessageFromExchange,
        >,
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
    pub async fn connect(&self) -> EGResult<()> {
        match &self.transport_client {
            TransportClient::Websocket(client) => client.connect().await,
            _ => Ok(()),
        }
    }
    pub async fn disconnect(&self) -> EGResult<()> {
        match &self.transport_client {
            TransportClient::Websocket(client) => client.disconnect().await,
            _ => Ok(()),
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
            (client, _) => Err(EGError::BadTransportType(client.transport_type())),
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
            (client, _) => Err(EGError::BadTransportType(client.transport_type())),
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
            (client, _) => Err(EGError::BadTransportType(client.transport_type())),
        }
    }
}
