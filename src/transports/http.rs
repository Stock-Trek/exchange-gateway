use crate::error::EGResult;
use async_trait::async_trait;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc, time::Duration};

pub type CreateHttpClient<T> = Arc<dyn Fn(&str) -> HttpClientMarker<T> + Send + Sync>;

pub type HttpClientMarker<T> = Arc<dyn HttpClientTrait<T>>;

#[async_trait]
pub trait HttpClientTrait<T>: Send + Sync {
    async fn send_message(
        &self,
        message: HttpMessageDto<T>,
        timeout: Duration,
    ) -> EGResult<HttpMessageDto<T>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpMessageDto<T> {
    pub headers: HashMap<String, String>,
    pub body: T,
}

impl<T> std::fmt::Display for HttpMessageDto<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HttpMessageDto( headers: {:?}, body: {} )",
            self.headers, self.body
        )
    }
}
