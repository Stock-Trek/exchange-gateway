use crate::{error::EGResult, sign::signer::Signer};
use std::sync::Arc;

pub type TryConvertRequestTo<TRequest, TMessageToExchange> =
    Arc<dyn Fn(&TRequest) -> EGResult<TMessageToExchange> + Send + Sync>;
pub type TryConvertResponseFrom<TMessageFromExchange, TResponse> =
    Arc<dyn Fn(TMessageFromExchange) -> EGResult<TResponse> + Send + Sync>;

pub(crate) type CreateAuthMessage<TUnsignedMessage> = fn() -> TUnsignedMessage;

pub(crate) type CreateSignerFrom<TFrom, TUnsignedMessage, TSignedMessage> =
    fn(TFrom) -> EGResult<Signer<TUnsignedMessage, TSignedMessage>>;
pub(crate) type SignatureAppender<TUnsignedMessage, TSignedMessage> =
    Arc<dyn Fn(TUnsignedMessage, Option<String>) -> TSignedMessage + Send + Sync>;

pub(crate) type TryConvertRef<From, To> = fn(&From) -> EGResult<To>;
pub(crate) type TryConvertValue<From, To> = fn(From) -> EGResult<To>;

pub fn double_converter<TFrom, TVia, TTo>(
    from_via: TryConvertResponseFrom<TFrom, TVia>,
    via_to: TryConvertResponseFrom<TVia, TTo>,
) -> TryConvertResponseFrom<TFrom, TTo>
where
    TFrom: 'static,
    TVia: 'static,
    TTo: 'static,
{
    Arc::new(move |from| {
        let via = from_via(from)?;
        via_to(via)
    })
}
pub fn triple_converter<TFrom, TVia0, TVia1, TTo>(
    from_via0: TryConvertResponseFrom<TFrom, TVia0>,
    via0_via1: TryConvertResponseFrom<TVia0, TVia1>,
    via1_to: TryConvertResponseFrom<TVia1, TTo>,
) -> TryConvertResponseFrom<TFrom, TTo>
where
    TFrom: 'static,
    TVia0: 'static,
    TVia1: 'static,
    TTo: 'static,
{
    Arc::new(move |from| {
        let via0 = from_via0(from)?;
        let via1 = via0_via1(via0)?;
        via1_to(via1)
    })
}

pub type ResponseConverter<TResponse, TConvertedResponse> =
    Arc<dyn Fn(TResponse) -> EGResult<TConvertedResponse> + Send + Sync>;

pub type FilterMessage<TMessage, TFiltered> =
    Arc<dyn Fn(&TMessage) -> Option<TFiltered> + Send + Sync>;
