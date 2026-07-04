use crate::{error::EGResult, sign::signer::Signer};

pub type TryConvertRequestTo<TRequest, TMessageToExchange> =
    Box<dyn Fn(&TRequest) -> EGResult<TMessageToExchange> + Send + Sync>;
pub type TryConvertResponseFrom<TMessageFromExchange, TResponse> =
    Box<dyn Fn(TMessageFromExchange) -> EGResult<TResponse> + Send + Sync>;

pub(crate) type CreateAuthMessage<TUnsignedMessage> = fn() -> TUnsignedMessage;

pub(crate) type CreateSignerFrom<TFrom, TUnsignedMessage, TSignedMessage> =
    fn(TFrom) -> EGResult<Signer<TUnsignedMessage, TSignedMessage>>;
pub(crate) type SignatureAppender<TUnsignedMessage, TSignedMessage> =
    Box<dyn Fn(TUnsignedMessage, Option<String>) -> TSignedMessage + Send + Sync>;

pub(crate) type TryConvertRef<From, To> = fn(&From) -> EGResult<To>;
pub(crate) type TryConvertValue<From, To> = fn(From) -> EGResult<To>;
