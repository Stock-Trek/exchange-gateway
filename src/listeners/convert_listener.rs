use crate::{error::EGResult, functions::TryConvertValue, listeners::listener::ListenerTrait};
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ConvertListener<TFrom, TTo> {
    converter: TryConvertValue<TFrom, TTo>,
    delegate: Arc<dyn ListenerTrait<TMessage = TTo>>,
}

impl<TFrom, TTo> ConvertListener<TFrom, TTo> {
    pub fn new(
        converter: TryConvertValue<TFrom, TTo>,
        delegate: impl ListenerTrait<TMessage = TTo> + 'static,
    ) -> Self {
        Self {
            converter,
            delegate: Arc::new(delegate),
        }
    }
}

#[async_trait]
impl<TFrom, TTo> ListenerTrait for ConvertListener<TFrom, TTo>
where
    TFrom: Send,
    TTo: Send,
{
    type TMessage = TFrom;

    async fn on_message(&self, message: TFrom) -> EGResult<()> {
        let converted = (self.converter)(message)?;
        self.delegate.on_message(converted).await
    }

    async fn on_connected(&self) -> EGResult<()> {
        self.delegate.on_connected().await
    }

    async fn on_disconnected(&self) -> EGResult<()> {
        self.delegate.on_disconnected().await
    }
}

impl<TFrom, TTo> std::fmt::Display for ConvertListener<TFrom, TTo> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConvertListener")
            .field("converter", &"<function>")
            .field("delegate", &"<Listener>")
            .finish()
    }
}
