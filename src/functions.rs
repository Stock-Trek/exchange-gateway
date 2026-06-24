use crate::{error::EGResult, sign::signer::Signer};

pub type CreateAuthMessage<TUnsignedMessage> = fn() -> TUnsignedMessage;

pub type CreateSignerFrom<TFrom, TUnsignedMessage, TSignedMessage> =
    fn(TFrom) -> EGResult<Signer<TUnsignedMessage, TSignedMessage>>;

pub type TryConvertFromRequest<TRequest, TMessageToExchange> =
    Box<dyn Fn(&TRequest) -> EGResult<TMessageToExchange> + Send + Sync>;

pub type SignatureAppender<TUnsignedMessage, TSignedMessage> =
    Box<dyn Fn(TUnsignedMessage, Option<String>) -> TSignedMessage + Send + Sync>;

pub type TryConvertToResponse<TMessageFromExchange, TResponse> =
    fn(TMessageFromExchange) -> EGResult<TResponse>;

pub type TryConvertRef<From, To> = fn(&From) -> EGResult<To>;
pub type TryConvertValue<From, To> = fn(From) -> EGResult<To>;
