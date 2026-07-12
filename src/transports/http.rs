use crate::error::EGResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub type CreateHttpClient = Arc<dyn Fn(&str) -> HttpClientMarker + Send + Sync>;

pub type HttpClientMarker = Arc<dyn HttpClientTrait>;

#[async_trait]
pub trait HttpClientTrait: Send + Sync {
    async fn send_message(
        &self,
        message: HttpMessageDto,
        timeout: Duration,
    ) -> EGResult<HttpMessageDto>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpMessageDto {
    pub headers: HashMap<String, String>,
    pub body_json: String,
}

impl std::fmt::Display for HttpMessageDto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HttpMessageDto( headers: {:?}, body: {} )",
            self.headers, self.body_json
        )
    }
}
