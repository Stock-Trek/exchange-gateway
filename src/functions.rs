use anyhow::Result;
use std::sync::Arc;

pub(crate) type ArcCombineValues<TValue0, TValue1, TCombined> =
    Arc<dyn Fn(TValue0, TValue1) -> TCombined + Send + Sync>;

pub type ArcTryConvertRef<TFrom, TTo> = Arc<dyn Fn(&TFrom) -> Result<TTo> + Send + Sync>;
pub type ArcTryConvertValue<TFrom, TTo> = Arc<dyn Fn(TFrom) -> Result<TTo> + Send + Sync>;

pub(crate) type TryConvertRef<From, To> = fn(&From) -> Result<To>;
pub(crate) type TryConvertValue<From, To> = fn(From) -> Result<To>;

pub(crate) fn double_converter<TFrom, TVia, TTo>(
    from_via: ArcTryConvertValue<TFrom, TVia>,
    via_to: ArcTryConvertValue<TVia, TTo>,
) -> ArcTryConvertValue<TFrom, TTo>
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
