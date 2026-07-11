use crate::error::EGResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub type CreateHttpClient = Arc<dyn Fn() -> HttpClientMarker + Send + Sync>;

pub type HttpClientMarker = Arc<dyn HttpClientTrait>;

#[async_trait]
pub trait HttpClientTrait: Send + Sync {
    async fn send_message(
        &self,
        message: HttpMessageDto,
        timeout: Duration,
    ) -> EGResult<HttpMessageDto>;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HttpMessageDto {
    pub headers: HashMap<String, String>,
    pub body_json: String,
}
