use crate::error::EGResult;
use std::sync::Arc;

pub(crate) type ArcCombineValues<TValue0, TValue1, TCombined> =
    Arc<dyn Fn(TValue0, TValue1) -> TCombined + Send + Sync>;

pub type ArcTryConvertRef<TFrom, TTo> = Arc<dyn Fn(&TFrom) -> EGResult<TTo> + Send + Sync>;
pub type ArcTryConvertValue<TFrom, TTo> = Arc<dyn Fn(TFrom) -> EGResult<TTo> + Send + Sync>;
pub type ArcConvertRef<TFrom, TTo> = Arc<dyn Fn(&TFrom) -> TTo + Send + Sync>;
pub type ArcCreate<TCreated> = Arc<dyn Fn() -> TCreated + Send + Sync>;
pub type ArcPredicate<T> = Arc<dyn for<'a> Fn(&'a T) -> bool + Send + Sync>;

pub(crate) type TryConvertRef<From, To> = fn(&From) -> EGResult<To>;
pub(crate) type TryConvertValue<From, To> = fn(From) -> EGResult<To>;

/// Correlates an unsigned request to the exchange response it expects: the
/// spec stamps any request metadata (e.g. an id) and returns the filter used
/// to match the reply. HTTP is always request/response, so the filter accepts
/// anything; WebSocket matches on the id the connector generated.
pub(crate) type ToFilter<EGUnsignedReq, EGRes> =
    fn(EGUnsignedReq) -> (EGUnsignedReq, ArcPredicate<EGRes>);
