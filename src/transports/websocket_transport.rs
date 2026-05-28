use crate::transports::transport::TransportTrait;

pub trait WebsocketTransportTrait: TransportTrait<MessageDto = WebsocketMessageDto> {}

pub struct WebsocketMessageDto {
    pub body_json: String,
}
