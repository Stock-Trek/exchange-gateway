use crate::authenticator::Authenticator;

pub trait AuthenticatorCreator<TRequest, TUnsignedMessage, TCredentials, TResponse> {
    fn into_authenticator(
        self,
    ) -> Authenticator<TRequest, TUnsignedMessage, TCredentials, TResponse>;
}
