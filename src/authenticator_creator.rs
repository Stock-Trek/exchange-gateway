use crate::{
    authenticator::Authenticator, converter::Converter, error::EGResult,
    listeners::listener::Listener,
};

pub trait AuthenticatorCreator<
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
        converter: Converter<TRequest, TUnsignedMessageToExchange, TMessageFromExchange, TResponse>,
        listener: Listener<TMessageDto>,
    ) -> EGResult<Authenticator<TRequest, TCredentials, TResponse>>;
}
