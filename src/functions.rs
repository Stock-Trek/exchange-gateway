use crate::error::EGResult;
use std::sync::Arc;

pub(crate) type ArcCombineValues<TValue0, TValue1, TCombined> =
    Arc<dyn Fn(TValue0, TValue1) -> TCombined + Send + Sync>;

pub type ArcTryConvertRef<TFrom, TTo> = Arc<dyn Fn(&TFrom) -> EGResult<TTo> + Send + Sync>;
pub type ArcTryConvertValue<TFrom, TTo> = Arc<dyn Fn(TFrom) -> EGResult<TTo> + Send + Sync>;
pub type ArcPredicate<T> = Arc<dyn for<'a> Fn(&'a T) -> bool + Send + Sync>;
/// Creates one authentication attempt: the message to send together with the
/// filter that matches its response. Both are produced per attempt so a retry
/// can use a fresh request id and bind the waiter to that attempt.
pub(crate) type ArcCreateAuthAttempt<EGUnsignedReq, EGRes> =
    Arc<dyn Fn() -> (EGUnsignedReq, ArcPredicate<EGRes>) + Send + Sync>;

pub(crate) type TryConvertRef<From, To> = fn(&From) -> EGResult<To>;
pub(crate) type TryConvertValue<From, To> = fn(From) -> EGResult<To>;
