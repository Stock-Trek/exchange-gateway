use crate::{
    error::EGResult,
    listeners::listener::Listener,
    transports::{transport::TransportTrait, transport_creator::TransportCreatorTrait},
};
use async_trait::async_trait;
use chrono::Duration;
use std::collections::HashMap;

pub struct HttpTransportCreator {
    pub create_client: Box<dyn Fn() -> HttpClient>,
}

pub type HttpClient = Box<dyn HttpClientTrait>;

#[async_trait]
pub trait HttpClientTrait: Send + Sync {
    async fn send_message(
        &self,
        message: HttpMessageDto,
        timeout: Duration,
    ) -> EGResult<HttpMessageDto>;
}

impl TransportCreatorTrait<HttpTransport, HttpMessageDto> for HttpTransportCreator {
    fn create_transport(&self, listener: Listener<HttpMessageDto>) -> EGResult<HttpTransport> {
        let client = (self.create_client)();
        Ok(HttpTransport { client, listener })
    }
}

#[derive(Clone)]
pub struct HttpMessageDto {
    pub headers: HashMap<String, String>,
    pub body_json: String,
}

pub(crate) struct HttpTransport {
    client: HttpClient,
    listener: Listener<HttpMessageDto>,
}

#[async_trait]
impl TransportTrait for HttpTransport {
    type MessageDto = HttpMessageDto;

    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()> {
        self.send_inner(message_dto, timeout).await
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
        self.send_and_wait_inner(message_dto, timeout, filter).await
    }
}

impl HttpTransport {
    pub fn new(
        client: crate::transports::http_transport::HttpClient,
        listener: Listener<HttpMessageDto>,
    ) -> Self {
        Self { client, listener }
    }
    pub(crate) async fn send_inner(
        &self,
        message_dto: HttpMessageDto,
        timeout: Duration,
    ) -> EGResult<()> {
        let future = self.client.send_message(message_dto, timeout);
        let response = future.await?;
        self.listener.on_message(response).await
    }
    pub(crate) async fn send_and_wait_inner<TResponse, F>(
        &self,
        dto: HttpMessageDto,
        timeout: Duration,
        filter: F,
    ) -> EGResult<TResponse>
    where
        F: Fn(&HttpMessageDto) -> Option<TResponse> + Send + Sync,
    {
        let response = self.client.send_message(dto, timeout).await?;
        filter(&response).ok_or_else(|| {
            crate::error::EGError::Custom("filter returned None for HTTP response".into())
        })
    }
}
