use crate::error::EGResult;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc, time::Duration};

pub type CreateHttpClient = Arc<dyn Fn() -> HttpClient>;

pub type HttpClient = Box<dyn HttpClientTrait>;

#[async_trait]
pub trait HttpClientTrait: Send + Sync {
    async fn send_message(
        &self,
        message: HttpMessageDto,
        timeout: Duration,
    ) -> EGResult<HttpMessageDto>;
}

#[derive(Clone)]
pub struct HttpMessageDto {
    pub headers: HashMap<String, String>,
    pub body_json: String,
}
