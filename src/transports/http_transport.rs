use crate::transports::transport::TransportTrait;
use std::collections::HashMap;

pub trait HttpTransportTrait: TransportTrait<MessageDto = HttpMessageDto> {}

pub struct HttpMessageDto {
    pub headers: HashMap<String, String>,
    pub body_json: String,
}
