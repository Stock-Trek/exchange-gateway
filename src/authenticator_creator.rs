use crate::{authenticator::Authenticator, error::EGResult, listeners::listener::Listener};

pub type AuthenticatorCreator<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageDto,
    TMessageFromExchange,
    TResponse,
> = Box<
    dyn AuthenticatorCreatorTrait<
            TRequest,
            TUnsignedMessageToExchange,
            TCredentials,
            TMessageDto,
            TMessageFromExchange,
            TResponse,
        >,
>;

pub trait AuthenticatorCreatorTrait<
    TRequest,
    TUnsignedMessageToExchange,
    TCredentials,
    TMessageDto,
    TMessageFromExchange,
    TResponse,
>
{
    fn into_authenticator(
        self,
        listener: Listener<TMessageDto>,
    ) -> EGResult<Authenticator<TRequest, TCredentials, TResponse>>;
}
