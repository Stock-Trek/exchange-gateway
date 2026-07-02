use crate::{
    error::EGResult,
    listeners::listener::Listener,
    transports::{transport::TransportTrait, transport_creator::TransportCreatorTrait},
};
use async_trait::async_trait;
use chrono::Duration;
use std::{collections::HashMap, sync::Arc};

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
    fn create_transport(&self, listener: Arc<Listener<HttpMessageDto>>) -> EGResult<HttpTransport> {
        Ok(HttpTransport {
            client: (self.create_client)(),
            listener,
        })
    }
}

#[derive(Clone)]
pub struct HttpMessageDto {
    pub headers: HashMap<String, String>,
    pub body_json: String,
}

pub struct HttpTransport {
    client: HttpClient,
    listener: Arc<Listener<HttpMessageDto>>,
}

#[async_trait]
impl TransportTrait for HttpTransport {
    type MessageDto = HttpMessageDto;
    async fn send(&self, message_dto: Self::MessageDto, timeout: Duration) -> EGResult<()> {
        let future = self.client.send_message(message_dto, timeout);
        let response = future.await?;
        self.listener.on_message(response).await
    }
}
