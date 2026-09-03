use crate::error::EGResult;
use std::sync::Arc;

pub type BoxTryCreateOnce<TFrom, TTo> = Box<dyn FnOnce(TFrom) -> EGResult<TTo> + Send + Sync>;

pub type ArcTryConvertRef<TFrom, TTo> = Arc<dyn Fn(&TFrom) -> EGResult<TTo> + Send + Sync>;
pub type ArcTryConvertValue<TFrom, TTo> = Arc<dyn Fn(TFrom) -> EGResult<TTo> + Send + Sync>;
pub type ArcPredicate<T> = Arc<dyn for<'a> Fn(&'a T) -> bool + Send + Sync>;

pub(crate) type TryConvertRef<From, To> = fn(&From) -> EGResult<To>;
pub(crate) type TryConvertValue<From, To> = fn(From) -> EGResult<To>;
