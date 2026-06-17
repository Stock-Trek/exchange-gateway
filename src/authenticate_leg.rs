use crate::{
    functions::{CreateAuthMessage, CreateSigner, DeserializeReply, FilterReply, MessageToDto},
    sign::signer::Signer,
    transports::transport::TransportTrait,
};
use async_trait::async_trait;
use chrono::Duration;
use std::sync::Arc;
use stock_trek::error::result::StockTrekResult;

pub type AuthenticateLeg<TUnsignedMessage, TSignedMessage> =
    Box<dyn AuthenticateLegTrait<TUnsignedMessage, TSignedMessage>>;

#[async_trait]
pub trait AuthenticateLegTrait<TUnsignedMessage, TSignedMessage>: Send + Sync {
    async fn do_leg(
        &self,
        signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>>;
}

pub struct AuthenticateLegImpl<
    TTransport,
    TUnsignedMessage,
    TSignedMessage,
    TRawReply,
    TAuthentication,
> where
    TTransport: TransportTrait + ?Sized,
{
    transport: Arc<TTransport>,
    timeout: Duration,
    create_auth_message: CreateAuthMessage<TUnsignedMessage>,
    to_dto: MessageToDto<TSignedMessage, TTransport::MessageDto>,
    deserialize_reply: DeserializeReply<TTransport::MessageDto, TRawReply>,
    filter_reply: FilterReply<TRawReply, TAuthentication>,
    create_signer: CreateSigner<TAuthentication, TUnsignedMessage, TSignedMessage>,
}

impl<TTransport, TUnsignedMessage, TSignedMessage, TRawReply, TAuthentication>
    AuthenticateLegImpl<TTransport, TUnsignedMessage, TSignedMessage, TRawReply, TAuthentication>
where
    TTransport: TransportTrait + ?Sized + 'static,
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TRawReply: Send + Sync + 'static,
    TAuthentication: Send + Sync + 'static,
{
    pub fn new(
        transport: Arc<TTransport>,
        timeout: Duration,
        create_auth_message: CreateAuthMessage<TUnsignedMessage>,
        to_dto: MessageToDto<TSignedMessage, TTransport::MessageDto>,
        deserialize_reply: DeserializeReply<TTransport::MessageDto, TRawReply>,
        filter_reply: FilterReply<TRawReply, TAuthentication>,
        create_signer: CreateSigner<TAuthentication, TUnsignedMessage, TSignedMessage>,
    ) -> AuthenticateLeg<TUnsignedMessage, TSignedMessage> {
        Box::new(Self {
            transport,
            timeout,
            create_auth_message,
            to_dto,
            deserialize_reply,
            filter_reply,
            create_signer,
        })
    }
}

#[async_trait]
impl<TTransport, TUnsignedMessage, TSignedMessage, TRawReply, TAuthentication>
    AuthenticateLegTrait<TUnsignedMessage, TSignedMessage>
    for AuthenticateLegImpl<
        TTransport,
        TUnsignedMessage,
        TSignedMessage,
        TRawReply,
        TAuthentication,
    >
where
    TTransport: TransportTrait + Send + Sync + ?Sized,
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
{
    async fn do_leg(
        &self,
        signer: Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>> {
        let signed_auth_message = signer.sign((self.create_auth_message)())?;
        let message = (self.to_dto)(&signed_auth_message)?;
        let reply_dto = self.transport.send(message, self.timeout).await?;
        let deserialized_reply = (self.deserialize_reply)(reply_dto)?;
        let authentication = (self.filter_reply)(deserialized_reply)?;
        Ok((self.create_signer)(&authentication))
    }
}
