use crate::{
    error::{EGError, EGResult},
    functions::TryConvertValue,
    listeners::listener::ListenerTrait,
};
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

    async fn on_connected(&self) -> EGResult<()> {
        self.delegate.on_connected().await
    }
    async fn on_disconnected(&self) -> EGResult<()> {
        self.delegate.on_disconnected().await
    }
    async fn on_error(&self, error: EGError) -> EGResult<()> {
        self.delegate.on_error(error).await
    }
    async fn on_message(&self, message: TFrom) -> EGResult<()> {
        match (self.converter)(message) {
            Ok(converted) => {
                if let Err(error) = self.delegate.on_message(converted).await {
                    self.delegate.on_error(error).await?;
                }
                Ok(())
            }
            Err(error) => self.delegate.on_error(error).await,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingListener {
        received: Arc<Mutex<Vec<u64>>>,
        errors: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl ListenerTrait for RecordingListener {
        type TMessage = u64;

        async fn on_message(&self, message: u64) -> EGResult<()> {
            self.received
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?
                .push(message);
            Ok(())
        }

        async fn on_error(&self, error: EGError) -> EGResult<()> {
            self.errors
                .lock()
                .map_err(|_| EGError::MutexPoisoned)?
                .push(error.to_string());
            Ok(())
        }
    }

    type Recording = (
        RecordingListener,
        Arc<Mutex<Vec<u64>>>,
        Arc<Mutex<Vec<String>>>,
    );

    fn recording() -> Recording {
        let received = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(Vec::new()));
        (
            RecordingListener {
                received: received.clone(),
                errors: errors.clone(),
            },
            received,
            errors,
        )
    }

    #[tokio::test]
    async fn conversion_failure_is_reported_through_on_error() {
        let (delegate, received, errors) = recording();
        let listener = ConvertListener::new(
            |_message: String| -> EGResult<u64> { Err(EGError::BadResponse) },
            delegate,
        );
        // A message that fails conversion is consumed, not forwarded ...
        listener.on_message("hello".to_string()).await.unwrap();
        assert!(received.lock().unwrap().is_empty());
        // ... and the failure is sent through `on_error` instead of being
        // silently dropped.
        assert_eq!(
            *errors.lock().unwrap(),
            vec![EGError::BadResponse.to_string()]
        );
    }

    #[tokio::test]
    async fn successful_conversion_is_forwarded_to_the_delegate() {
        let (delegate, received, errors) = recording();
        let listener = ConvertListener::new(
            |message: String| -> EGResult<u64> {
                message.parse().map_err(|_| EGError::BadResponse)
            },
            delegate,
        );
        listener.on_message("7".to_string()).await.unwrap();
        assert_eq!(*received.lock().unwrap(), vec![7]);
        assert!(errors.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn delegate_message_failure_is_reported_through_on_error() {
        let errors = Arc::new(Mutex::new(Vec::new()));
        struct FailingListener {
            errors: Arc<Mutex<Vec<String>>>,
        }
        #[async_trait]
        impl ListenerTrait for FailingListener {
            type TMessage = u64;

            async fn on_message(&self, _message: u64) -> EGResult<()> {
                Err(EGError::BadResponse)
            }

            async fn on_error(&self, error: EGError) -> EGResult<()> {
                self.errors
                    .lock()
                    .map_err(|_| EGError::MutexPoisoned)?
                    .push(error.to_string());
                Ok(())
            }
        }
        let listener = ConvertListener::new(
            Ok,
            FailingListener {
                errors: errors.clone(),
            },
        );
        // The delegate itself fails: the failure is reported through
        // `on_error` rather than dropped by a wrapping transport.
        listener.on_message(7u64).await.unwrap();
        assert_eq!(
            *errors.lock().unwrap(),
            vec![EGError::BadResponse.to_string()]
        );
    }

    #[tokio::test]
    async fn on_error_is_forwarded_to_the_delegate() {
        let (delegate, _received, errors) = recording();
        let listener = ConvertListener::new(Ok, delegate);
        listener.on_error(EGError::NotConnected).await.unwrap();
        assert_eq!(
            *errors.lock().unwrap(),
            vec![EGError::NotConnected.to_string()]
        );
    }
}
