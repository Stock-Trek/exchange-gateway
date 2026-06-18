use crate::{
    functions::{CreateAuthMessage, CreateSignerFrom},
    messenger::Messenger,
    sign::signer::Signer,
};
use async_trait::async_trait;
use stock_trek::error::result::StockTrekResult;

pub type AuthenticateLeg<TUnsignedMessage, TSignedMessage> =
    Box<dyn AuthenticateLegTrait<TUnsignedMessage, TSignedMessage>>;

#[async_trait]
pub trait AuthenticateLegTrait<TUnsignedMessage, TSignedMessage>: Send + Sync {
    async fn do_leg(
        &self,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>>;
}

pub struct AuthenticateLegImpl<TUnsignedMessage, TSignedMessage, TAuthentication> {
    create_auth_message: CreateAuthMessage<TUnsignedMessage>,
    messenger: Messenger<TSignedMessage, TAuthentication>,
    create_signer_from: CreateSignerFrom<TAuthentication, TUnsignedMessage, TSignedMessage>,
}

impl<TUnsignedMessage, TSignedMessage, TAuthentication>
    AuthenticateLegImpl<TUnsignedMessage, TSignedMessage, TAuthentication>
where
    TUnsignedMessage: Send + Sync + 'static,
    TSignedMessage: Send + Sync + 'static,
    TAuthentication: Send + Sync + 'static,
{
    pub fn new(
        create_auth_message: CreateAuthMessage<TUnsignedMessage>,
        messenger: Messenger<TSignedMessage, TAuthentication>,
        create_signer_from: CreateSignerFrom<TAuthentication, TUnsignedMessage, TSignedMessage>,
    ) -> AuthenticateLeg<TUnsignedMessage, TSignedMessage> {
        Box::new(Self {
            create_auth_message,
            messenger,
            create_signer_from,
        })
    }
}

#[async_trait]
impl<TUnsignedMessage, TSignedMessage, TAuthentication>
    AuthenticateLegTrait<TUnsignedMessage, TSignedMessage>
    for AuthenticateLegImpl<TUnsignedMessage, TSignedMessage, TAuthentication>
where
    TUnsignedMessage: Send + Sync,
    TSignedMessage: Send + Sync,
{
    async fn do_leg(
        &self,
        signer: &Signer<TUnsignedMessage, TSignedMessage>,
    ) -> StockTrekResult<Signer<TUnsignedMessage, TSignedMessage>> {
        let auth_message = (self.create_auth_message)();
        let signed_auth_message = signer.sign(auth_message)?;
        let authentication = self.messenger.send(signed_auth_message).await?;
        let signer = (self.create_signer_from)(&authentication);
        Ok(signer)
    }
}
