use crate::{authenticator::Authenticator, error::EGResult};

pub(crate) trait AuthenticatorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageDto,
    TMessageFromExchange,
    TResponse,
>
{
    fn into_authenticator(self) -> EGResult<Authenticator<TRequest, TCredentials, TResponse>>;
}
