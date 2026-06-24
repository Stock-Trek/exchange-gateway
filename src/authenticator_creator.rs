use crate::authenticator::Authenticator;

pub trait AuthenticatorCreator<
    TUnsignedMessage,
    TCredentials,
    TMessageToExchange,
    TMessageFromExchange,
    TResponse,
>
{
    fn into_authenticator(
        self,
    ) -> Authenticator<
        TUnsignedMessage,
        TCredentials,
        TMessageToExchange,
        TMessageFromExchange,
        TResponse,
    >;
}
